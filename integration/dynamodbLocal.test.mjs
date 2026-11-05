// Runs against DynamoDB Local:
//   docker run --rm -p 8000:8000 amazon/dynamodb-local -jar DynamoDBLocal.jar -inMemory
//   npm run test:integration
import assert from "node:assert/strict";
import { after, before, describe, test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const AWS = require("aws-sdk");
const { FailClosed, FailOpen } = require("../index.js");

const endpoint = process.env.DYNAMODB_ENDPOINT || "http://localhost:8000";
const awsConfig = {
    endpoint,
    region: "us-east-1",
    credentials: new AWS.Credentials("local", "local")
};
const rawDynamodb = new AWS.DynamoDB(awsConfig);
const dynamodb = new AWS.DynamoDB.DocumentClient(awsConfig);

const FAIL_OPEN_TABLE = `fail-open-${Date.now()}`;
const FAIL_CLOSED_TABLE = `fail-closed-${Date.now()}`;
const LEASE_MS = 1000;

function failOpen(overrides = {})
{
    return new FailOpen(
        {
            dynamodb,
            lockTable: FAIL_OPEN_TABLE,
            partitionKey: "id",
            leaseDurationMs: LEASE_MS,
            heartbeatPeriodMs: Math.floor(LEASE_MS / 3),
            trustLocalTime: true,
            retryCount: 0,
            maximumDurationMs: 30000,
            errorOnHeartbeatFailure: true,
            ...overrides
        }
    );
}

function acquire(client, id)
{
    return new Promise((resolve, reject) =>
        client.acquireLock(id, (error, lock) => (error ? reject(error) : resolve(lock)))
    );
}

function release(lock)
{
    return new Promise((resolve, reject) =>
        lock.release(error => (error ? reject(error) : resolve()))
    );
}

async function getRow(table, id)
{
    const { Item } = await dynamodb.get({ TableName: table, Key: { id }, ConsistentRead: true }).promise();
    return Item;
}

async function createTable(name)
{
    await rawDynamodb.createTable(
        {
            TableName: name,
            AttributeDefinitions: [{ AttributeName: "id", AttributeType: "S" }],
            KeySchema: [{ AttributeName: "id", KeyType: "HASH" }],
            BillingMode: "PAY_PER_REQUEST"
        }
    ).promise();
}

before(async () =>
{
    await createTable(FAIL_OPEN_TABLE);
    await createTable(FAIL_CLOSED_TABLE);
});

after(async () =>
{
    await rawDynamodb.deleteTable({ TableName: FAIL_OPEN_TABLE }).promise();
    await rawDynamodb.deleteTable({ TableName: FAIL_CLOSED_TABLE }).promise();
});

describe("config validation", () =>
{
    test("rejects a reserved partitionKey", () =>
    {
        assert.throws(() => failOpen({ partitionKey: "guid" }), /"partitionKey" contains an invalid value/);
    });

    test("rejects a missing leaseDurationMs", () =>
    {
        assert.throws(() => failOpen({ leaseDurationMs: undefined }), /"leaseDurationMs" is required/);
    });

    test("rejects a non-integer retryCount without converting strings", () =>
    {
        assert.throws(() => failOpen({ retryCount: "3" }), /"retryCount" must be a number/);
    });

    test("rejects a dynamodb client missing delete", () =>
    {
        assert.throws(
            () => failOpen({ dynamodb: { get() {}, put() {} } }),
            /"dynamodb.delete" is required/
        );
    });
});

describe("FailOpen", () =>
{
    test("acquires a free lock with fencing token 1 and writes its guid", async () =>
    {
        const lock = await acquire(failOpen(), "free");
        const row = await getRow(FAIL_OPEN_TABLE, "free");
        assert.equal(lock.fencingToken, 1);
        assert.equal(row.fencingToken, 1);
        assert.equal(row.leaseDurationMs, LEASE_MS);
        assert.ok(Buffer.from(row.guid).equals(lock._guid));
        await release(lock);
    });

    test("a held lock fails a non-retrying acquire with FailedToAcquireLock", async () =>
    {
        const holder = await acquire(failOpen(), "contended");
        await assert.rejects(acquire(failOpen(), "contended"), { code: "FailedToAcquireLock" });
        await release(holder);
    });

    test("heartbeats rotate the guid and keep the lease fresh", async () =>
    {
        const lock = await acquire(failOpen(), "heartbeat");
        const firstGuid = Buffer.from((await getRow(FAIL_OPEN_TABLE, "heartbeat")).guid);
        await sleep(LEASE_MS * 2);
        const row = await getRow(FAIL_OPEN_TABLE, "heartbeat");
        assert.ok(!Buffer.from(row.guid).equals(firstGuid));
        assert.ok(Date.now() - row.lockAcquiredTimeUnixMs < LEASE_MS);
        await release(lock);
    });

    test("release shortens the lease so the next owner gets fencing token + 1", async () =>
    {
        const first = await acquire(failOpen(), "released");
        await release(first);
        assert.equal((await getRow(FAIL_OPEN_TABLE, "released")).leaseDurationMs, 1);

        const started = Date.now();
        const second = await acquire(failOpen(), "released");
        assert.equal(second.fencingToken, first.fencingToken + 1);
        assert.ok(Date.now() - started < LEASE_MS);
        await release(second);
    });

    test("a holder that stops heartbeating loses the lock after its lease", async () =>
    {
        const dead = await acquire(failOpen(), "dead-holder");
        clearTimeout(dead.heartbeatTimeout);

        const takeover = await acquire(failOpen(), "dead-holder");
        assert.equal(takeover.fencingToken, dead.fencingToken + 1);
        await release(takeover);

        // A stale release must not clobber the new owner's row.
        await release(dead);
        const row = await getRow(FAIL_OPEN_TABLE, "dead-holder");
        assert.ok(Buffer.from(row.guid).equals(takeover._guid));
    });

    test("stops heartbeating after maximumDurationMs and emits lockExpired", async () =>
    {
        const capped = await acquire(failOpen({ maximumDurationMs: LEASE_MS }), "capped");
        const expired = new Promise(resolve => capped.once("lockExpired", resolve));
        const event = await expired;
        assert.equal(event.partitionID, "capped");

        const takeover = await acquire(failOpen(), "capped");
        assert.equal(takeover.fencingToken, capped.fencingToken + 1);
        await release(takeover);
        await release(capped);
    });

    test("emits error when a heartbeat write fails", async () =>
    {
        const lock = await acquire(failOpen(), "heartbeat-error");
        const failure = new Promise(resolve => lock.once("error", resolve));
        await dynamodb.put({ TableName: FAIL_OPEN_TABLE, Item: { id: "heartbeat-error", guid: "stolen" } }).promise();
        const error = await failure;
        assert.equal(error.code, "ConditionalCheckFailedException");
        clearTimeout(lock.heartbeatTimeout);
    });
});

describe("FailClosed", () =>
{
    function failClosed()
    {
        return new FailClosed(
            { dynamodb, lockTable: FAIL_CLOSED_TABLE, partitionKey: "id", acquirePeriodMs: 100, retryCount: 0 }
        );
    }

    test("acquire, contend, release, reacquire", async () =>
    {
        const lock = await acquire(failClosed(), "closed");
        await assert.rejects(acquire(failClosed(), "closed"), { code: "FailedToAcquireLock" });
        await release(lock);
        assert.equal(await getRow(FAIL_CLOSED_TABLE, "closed"), undefined);
        await release(await acquire(failClosed(), "closed"));
    });
});
