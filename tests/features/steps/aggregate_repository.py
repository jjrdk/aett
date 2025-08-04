import datetime
import sqlite3

import aiosqlite
import time
import uuid

import asyncpg
import aiomysql
import boto3
import psycopg
import pymysql
from aioboto3 import Session
from behave import *
from behave.api.async_step import async_run_until_complete
from pydantic_core import to_json
from pymongo import MongoClient, AsyncMongoClient

from aett.domain import AsyncDefaultAggregateRepository, DefaultAggregateRepository
from aett.eventstore import EventMessage, Commit
from test_types import TestAggregate, TestEvent, TestMemento
from storage_factory import (
    create_async_commit_store,
    create_async_snapshot_store,
    create_commit_store,
    create_snapshot_store,
)

use_step_matcher("re")


@given("I have a persistent aggregate repository")
def step_impl(context):
    context.stream_id = str(uuid.uuid4())
    context.tenant_id = str(uuid.uuid4())
    commit_store = create_commit_store(
        context.db, context.storage_type, context.topic_map, context=context
    )
    snapshot_store = create_snapshot_store(
        context.db, context.storage_type, context=context
    )
    context.repository = DefaultAggregateRepository(
        tenant_id=context.tenant_id, store=commit_store, snapshot_store=snapshot_store
    )


@given("I have a persistent async aggregate repository")
def step_impl(context):
    context.stream_id = str(uuid.uuid4())
    context.tenant_id = str(uuid.uuid4())
    commit_store = create_async_commit_store(
        context.db, context.storage_type, context.topic_map, context=context
    )
    snapshot_store = create_async_snapshot_store(
        context.db, context.storage_type, context=context
    )
    context.repository = AsyncDefaultAggregateRepository(
        tenant_id=context.tenant_id, store=commit_store, snapshot_store=snapshot_store
    )


@then("a specific aggregate type can be loaded from the repository")
def step_impl(context):
    aggregate = context.repository.get(TestAggregate, context.stream_id)
    assert isinstance(aggregate, TestAggregate)


@then("a specific aggregate type can be loaded async from the repository")
@async_run_until_complete
async def step_impl(context):
    aggregate = await context.repository.get(TestAggregate, context.stream_id)
    assert isinstance(aggregate, TestAggregate)


@step("an aggregate is loaded from the repository and modified")
def step_impl(context):
    aggregate: TestAggregate = context.repository.get(TestAggregate, context.stream_id)
    aggregate.set_value(10)
    context.aggregate = aggregate


@step("an aggregate is loaded async from the repository and modified")
@async_run_until_complete
async def step_impl(context):
    aggregate: TestAggregate = await context.repository.get(
        TestAggregate, context.stream_id
    )
    aggregate.set_value(10)
    context.aggregate = aggregate


@step("an aggregate is saved async to the repository")
@async_run_until_complete
async def step_impl(context):
    await context.repository.save(context.aggregate)


@step("an aggregate is saved to the repository")
def step_impl(context):
    context.repository.save(context.aggregate)


@then("the modified is saved async to storage")
@async_run_until_complete
async def step_impl(context):
    m = await context.repository.get(TestAggregate, context.stream_id)
    assert m.value == 10


@then("the modified is saved to storage")
def step_impl(context):
    m = context.repository.get(TestAggregate, context.stream_id)
    assert m.value == 10


@step("loaded again async")
@async_run_until_complete
async def step_impl(context):
    a = await context.repository.get(TestAggregate, context.stream_id)
    context.aggregate = a


@step("loaded again")
def step_impl(context):
    a = context.repository.get(TestAggregate, context.stream_id)
    context.aggregate = a


@then("the modified aggregate is loaded from storage")
def step_impl(context):
    m: TestMemento = context.aggregate.get_memento()
    assert m.payload["key"] == 1010


@when("a series of commits is persisted async")
@async_run_until_complete
async def step_impl(context):
    start_time = datetime.datetime.fromtimestamp(0, datetime.timezone.utc)
    for x in range(1, 10):
        time_stamp = start_time + datetime.timedelta(days=x)
        match context.storage_type:
            case "sqlite_async":
                await seed_sqlite_async(context, x, time_stamp)
            case "mongo_async":
                await seed_mongo_async(context, x, time_stamp)
            case "postgres_async":
                await seed_postgres_async(context, x, time_stamp)
            case "mysql_async":
                await seed_mysql_async(context, x, time_stamp)
            case "dynamodb_async":
                await seed_dynamo_async(context, x, time_stamp)
            case "s3_async":
                await seed_s3_async(context, x, time_stamp)


@when("a series of commits is persisted")
def step_impl(context):
    start_time = datetime.datetime.fromtimestamp(0, datetime.timezone.utc)
    for x in range(1, 10):
        time_stamp = start_time + datetime.timedelta(days=x)
        match context.storage_type:
            case "dynamo":
                seed_dynamo(context, x, time_stamp)
            case "inmemory":
                seed_inmemory(context, x, time_stamp)
            case "sqlite":
                seed_sqlite(context, x, time_stamp)
            case "mongo":
                seed_mongo(context, x, time_stamp)
            case "postgres":
                seed_postgres(context, x, time_stamp)
            case "mysql":
                seed_mysql(context, x, time_stamp)
            case "s3":
                seed_s3(context, x, time_stamp)


def seed_dynamo(context, x, time_stamp):
    session = boto3.Session(
        aws_access_key_id="dummy",
        aws_secret_access_key="dummy",
        aws_session_token="dummy",
    )
    resource = session.resource(
        "dynamodb",
        region_name="localhost",
        endpoint_url=f"http://localhost:{context.db}",
    )
    table = resource.Table("commits")
    item = {
        "TenantAndStream": f"{context.tenant_id}{context.stream_id}",
        "TenantId": context.tenant_id,
        "StreamId": context.stream_id,
        "StreamRevision": x,
        "CommitId": str(uuid.uuid4()),
        "CommitSequence": x,
        "CommitStamp": int(time_stamp.timestamp()),
        "Headers": to_json({}),
        "Events": to_json(
            [
                e.to_json()
                for e in [
                    EventMessage(
                        body=TestEvent(
                            source=context.stream_id,
                            timestamp=time_stamp,
                            version=x - 1,
                            value=x,
                        )
                    )
                ]
            ]
        ),
    }
    _ = table.put_item(
        Item=item,
        ReturnValues="NONE",
        ReturnValuesOnConditionCheckFailure="NONE",
        ConditionExpression="attribute_not_exists(TenantAndStream) AND attribute_not_exists(CommitSequence)",
    )


async def seed_dynamo_async(context, x, time_stamp):
    session = Session(
        aws_access_key_id="dummy",
        aws_secret_access_key="dummy",
        aws_session_token="dummy",
    )
    async with session.resource(
        "dynamodb",
        region_name="localhost",
        endpoint_url=f"http://localhost:{context.db}",
    ) as resource:
        table = await resource.Table("commits")
        item = {
            "TenantAndStream": f"{context.tenant_id}{context.stream_id}",
            "TenantId": context.tenant_id,
            "StreamId": context.stream_id,
            "StreamRevision": x,
            "CommitId": str(uuid.uuid4()),
            "CommitSequence": x,
            "CommitStamp": int(time_stamp.timestamp()),
            "Headers": to_json({}),
            "Events": to_json(
                [
                    e.to_json()
                    for e in [
                        EventMessage(
                            body=TestEvent(
                                source=context.stream_id,
                                timestamp=time_stamp,
                                version=x - 1,
                                value=x,
                            )
                        )
                    ]
                ]
            ),
        }
        _ = await table.put_item(
            Item=item,
            ReturnValues="NONE",
            ReturnValuesOnConditionCheckFailure="NONE",
            ConditionExpression="attribute_not_exists(TenantAndStream) AND attribute_not_exists(CommitSequence)",
        )


def seed_inmemory(context, x, time_stamp):
    context.repository._store._ensure_stream(context.tenant_id, context.stream_id)
    persistence = context.repository._store._buckets[context.tenant_id][
        context.stream_id
    ]
    commit = Commit(
        tenant_id=context.tenant_id,
        stream_id=context.stream_id,
        commit_stamp=time_stamp,
        commit_sequence=x,
        stream_revision=1,
        events=[
            EventMessage(
                body=TestEvent(
                    source=context.stream_id,
                    timestamp=time_stamp,
                    version=x - 1,
                    value=x,
                )
            )
        ],
        headers={},
        checkpoint_token=0,
        commit_id=uuid.uuid4(),
    )
    persistence.append(commit)


async def seed_mongo_async(context, x, time_stamp):
    client = AsyncMongoClient(context.db)
    collection = client.get_database("test").get_collection("commits")
    doc = {
        "TenantId": context.tenant_id,
        "StreamId": context.stream_id,
        "StreamRevision": x,
        "CommitId": str(uuid.uuid4()),
        "CommitSequence": x,
        "CommitStamp": int(time_stamp.timestamp()),
        "Headers": to_json({}),
        "Events": to_json(
            [
                e.to_json()
                for e in [
                    EventMessage(
                        body=TestEvent(
                            source=context.stream_id,
                            timestamp=time_stamp,
                            version=x - 1,
                            value=x,
                        )
                    )
                ]
            ]
        ),
        "CheckpointToken": x,
    }
    _ = await collection.insert_one(doc)


def seed_mongo(context, x, time_stamp):
    client = MongoClient(context.db)
    collection = client.get_database("test").get_collection("commits")
    doc = {
        "TenantId": context.tenant_id,
        "StreamId": context.stream_id,
        "StreamRevision": x,
        "CommitId": str(uuid.uuid4()),
        "CommitSequence": x,
        "CommitStamp": int(time_stamp.timestamp()),
        "Headers": to_json({}),
        "Events": to_json(
            [
                e.to_json()
                for e in [
                    EventMessage(
                        body=TestEvent(
                            source=context.stream_id,
                            timestamp=time_stamp,
                            version=x - 1,
                            value=x,
                        )
                    )
                ]
            ]
        ),
        "CheckpointToken": x,
    }
    _ = collection.insert_one(doc)


def seed_postgres(context, x, time_stamp):
    with psycopg.connect(context.db) as conn:
        conn.execute(
            """INSERT
               INTO commits
               (TenantId, StreamId, StreamIdOriginal, CommitId, CommitSequence, StreamRevision, Items, CommitStamp,
                Headers, Payload)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
               RETURNING CheckpointNumber;""",
            (
                context.tenant_id,
                context.stream_id,
                context.stream_id,
                str(uuid.uuid4()),
                x,
                x,
                1,
                time_stamp,
                to_json({}),
                to_json(
                    [
                        e.to_json()
                        for e in [
                            EventMessage(
                                body=TestEvent(
                                    source=context.stream_id,
                                    timestamp=time_stamp,
                                    version=x - 1,
                                    value=x,
                                )
                            )
                        ]
                    ]
                ),
            ),
        )


async def seed_postgres_async(context, x, time_stamp):
    conn = await asyncpg.connect(context.db)
    await conn.execute(
        """INSERT
           INTO commits
           (TenantId, StreamId, StreamIdOriginal, CommitId, CommitSequence, StreamRevision, Items, CommitStamp, Headers,
            Payload)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
           RETURNING CheckpointNumber;""",
        context.tenant_id,
        context.stream_id,
        context.stream_id,
        str(uuid.uuid4()),
        x,
        x,
        1,
        time_stamp,
        to_json({}),
        to_json(
            [
                e.to_json()
                for e in [
                    EventMessage(
                        body=TestEvent(
                            source=context.stream_id,
                            timestamp=time_stamp,
                            version=x - 1,
                            value=x,
                        )
                    )
                ]
            ]
        ),
    )


def seed_mysql(context, x, time_stamp):
    with pymysql.connect(
        host=context.host,
        user=context.user,
        password=context.password,
        database=context.database,
        port=context.port,
        autocommit=True,
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """INSERT
                   INTO commits
                   (TenantId, StreamId, StreamIdOriginal, CommitId, CommitSequence, StreamRevision, Items, CommitStamp,
                    Headers, Payload)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""",
                (
                    context.tenant_id,
                    context.stream_id,
                    context.stream_id,
                    uuid.uuid4().bytes,
                    x,
                    x,
                    1,
                    time_stamp,
                    to_json({}),
                    to_json(
                        [
                            e.to_json()
                            for e in [
                                EventMessage(
                                    body=TestEvent(
                                        source=context.stream_id,
                                        timestamp=time_stamp,
                                        version=x - 1,
                                        value=x,
                                    )
                                )
                            ]
                        ]
                    ),
                ),
            )


async def seed_mysql_async(context, x, time_stamp):
    async with aiomysql.connect(
        host=context.host,
        user=context.user,
        password=context.password,
        db=context.database,
        port=context.port,
        autocommit=True,
    ) as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(
                """INSERT
                   INTO commits
                   (TenantId, StreamId, StreamIdOriginal, CommitId, CommitSequence, StreamRevision, Items, CommitStamp,
                    Headers, Payload)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""",
                (
                    context.tenant_id,
                    context.stream_id,
                    context.stream_id,
                    uuid.uuid4().bytes,
                    x,
                    x,
                    1,
                    time_stamp,
                    to_json({}),
                    to_json(
                        [
                            e.to_json()
                            for e in [
                                EventMessage(
                                    body=TestEvent(
                                        source=context.stream_id,
                                        timestamp=time_stamp,
                                        version=x - 1,
                                        value=x,
                                    )
                                )
                            ]
                        ]
                    ),
                ),
            )


def seed_sqlite(context, x, time_stamp):
    with sqlite3.connect(context.db) as conn:
        cur = conn.cursor()
        cur.execute(
            """INSERT
               INTO commits
               (TenantId, StreamId, StreamIdOriginal, CommitId, CommitSequence, StreamRevision, Items, CommitStamp,
                Headers, Payload)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               RETURNING CheckpointNumber;""",
            (
                context.tenant_id,
                context.stream_id,
                context.stream_id,
                str(uuid.uuid4()),
                x,
                x,
                1,
                time_stamp,
                to_json({}),
                to_json(
                    [
                        e.to_json()
                        for e in [
                            EventMessage(
                                body=TestEvent(
                                    source=context.stream_id,
                                    timestamp=time_stamp,
                                    version=x - 1,
                                    value=x,
                                )
                            )
                        ]
                    ]
                ),
            ),
        )
        cur.close()
        conn.commit()


async def seed_sqlite_async(context, x, time_stamp):
    async with aiosqlite.connect(context.db) as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """INSERT
                   INTO commits
                   (TenantId, StreamId, StreamIdOriginal, CommitId, CommitSequence, StreamRevision, Items, CommitStamp,
                    Headers, Payload)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   RETURNING CheckpointNumber;""",
                (
                    context.tenant_id,
                    context.stream_id,
                    context.stream_id,
                    str(uuid.uuid4()),
                    x,
                    x,
                    1,
                    time_stamp,
                    to_json({}),
                    to_json(
                        [
                            e.to_json()
                            for e in [
                                EventMessage(
                                    body=TestEvent(
                                        source=context.stream_id,
                                        timestamp=time_stamp,
                                        version=x - 1,
                                        value=x,
                                    )
                                )
                            ]
                        ]
                    ),
                ),
            )
        await conn.commit()


def seed_s3(context, x, time_stamp):
    commit = Commit(
        tenant_id=context.tenant_id,
        stream_id=context.stream_id,
        commit_stamp=time_stamp,
        commit_sequence=x,
        stream_revision=1,
        events=[
            EventMessage(
                body=TestEvent(
                    source=context.stream_id,
                    timestamp=time_stamp,
                    version=x - 1,
                    value=x,
                )
            )
        ],
        headers={},
        checkpoint_token=0,
        commit_id=uuid.uuid4(),
    )
    commit_key = f"commits/{commit.tenant_id}/{commit.stream_id}/{int(commit.commit_stamp.timestamp())}_{commit.commit_sequence}_{commit.stream_revision}.json"
    d = commit.__dict__
    d["events"] = [e.to_json() for e in commit.events]
    d["headers"] = {k: to_json(v) for k, v in commit.headers.items()}
    body = to_json(d)
    client = context.db.to_client()
    client.put_object(
        Bucket=context.db.bucket,
        Key=commit_key,
        Body=body,
        ContentLength=len(body),
        Metadata={k: to_json(v) for k, v in commit.headers.items()},
    )


async def seed_s3_async(context, x, time_stamp):
    commit = Commit(
        tenant_id=context.tenant_id,
        stream_id=context.stream_id,
        commit_stamp=time_stamp,
        commit_sequence=x,
        stream_revision=1,
        events=[
            EventMessage(
                body=TestEvent(
                    source=context.stream_id,
                    timestamp=time_stamp,
                    version=x - 1,
                    value=x,
                )
            )
        ],
        headers={},
        checkpoint_token=0,
        commit_id=uuid.uuid4(),
    )
    commit_key = f"commits/{commit.tenant_id}/{commit.stream_id}/{int(commit.commit_stamp.timestamp())}_{commit.commit_sequence}_{commit.stream_revision}.json"
    d = commit.__dict__
    d["events"] = [e.to_json() for e in commit.events]
    d["headers"] = {k: to_json(v) for k, v in commit.headers.items()}
    body = to_json(d)
    async with context.db.to_client() as client:
        await client.put_object(
            Bucket=context.db.bucket,
            Key=commit_key,
            Body=body,
            ContentLength=len(body),
            Metadata={k: to_json(v) for k, v in commit.headers.items()},
        )


@step("a specific aggregate is loaded async at a specific time")
@async_run_until_complete
async def step_impl(context):
    date_to_load = datetime.datetime.fromtimestamp(
        0, datetime.timezone.utc
    ) + datetime.timedelta(days=5, hours=12)
    repo: AsyncDefaultAggregateRepository = context.repository
    context.aggregate = await repo.get_to(
        TestAggregate, context.stream_id, date_to_load
    )


@step("a specific aggregate is loaded at a specific time")
def step_impl(context):
    date_to_load = datetime.datetime.fromtimestamp(
        0, datetime.timezone.utc
    ) + datetime.timedelta(days=5, hours=12)
    repo: DefaultAggregateRepository = context.repository
    context.aggregate = repo.get_to(TestAggregate, context.stream_id, date_to_load)


@then("the aggregate is loaded at version (\\d+)")
def step_impl(context, version):
    agg: TestAggregate = context.aggregate
    assert agg.version == int(version), (
        f"Expected version {version} but got {agg.version}"
    )


@when("(\\d+) events are persisted async to an aggregate")
@async_run_until_complete
async def step_impl(context, count):
    start_time = time.time()
    for i in range(0, int(count)):
        agg: TestAggregate = await context.repository.get(
            TestAggregate, context.stream_id
        )
        agg.raise_event(
            TestEvent(
                source=context.stream_id,
                timestamp=datetime.datetime.now(datetime.timezone.utc),
                version=i + 1,
                value=i,
            )
        )
        await context.repository.save(agg)


@when("(\\d+) events are persisted to an aggregate")
def step_impl(context, count):
    for i in range(0, int(count)):
        agg: TestAggregate = context.repository.get(TestAggregate, context.stream_id)
        agg.raise_event(
            TestEvent(
                source=context.stream_id,
                timestamp=datetime.datetime.now(datetime.timezone.utc),
                version=i + 1,
                value=i,
            )
        )
        context.repository.save(agg)


@when("an aggregate is created async from multiple events")
@async_run_until_complete
async def step_impl(context):
    agg: TestAggregate = TestAggregate(context.stream_id, 0, None)
    for i in range(0, 10):
        agg.add_value(1)
    repo: AsyncDefaultAggregateRepository = context.repository
    await repo.save(agg)


@when("an aggregate is created from multiple events")
@async_run_until_complete
async def step_impl(context):
    agg: TestAggregate = TestAggregate(context.stream_id, 0, None)
    for i in range(0, 10):
        agg.add_value(1)
    repo: DefaultAggregateRepository = context.repository
    repo.save(agg)


@step("the aggregate is snapshotted async")
@async_run_until_complete
async def step_impl(context):
    repo: AsyncDefaultAggregateRepository = context.repository
    await repo.snapshot(TestAggregate, context.stream_id)


@step("the aggregate is snapshotted")
def step_impl(context):
    repo: DefaultAggregateRepository = context.repository
    repo.snapshot(TestAggregate, context.stream_id)


@step("additional events are added async")
@async_run_until_complete
async def step_impl(context):
    repo: AsyncDefaultAggregateRepository = context.repository
    agg: TestAggregate = await repo.get(TestAggregate, context.stream_id)
    agg.add_value(1)
    await repo.save(agg)


@step("additional events are added")
def step_impl(context):
    repo: DefaultAggregateRepository = context.repository
    agg: TestAggregate = repo.get(TestAggregate, context.stream_id)
    agg.add_value(1)
    repo.save(agg)


@step("the latest version is loaded async")
@async_run_until_complete
async def step_impl(context):
    context.aggregate = await context.repository.get(TestAggregate, context.stream_id)


@step("the latest version is loaded")
def step_impl(context):
    context.aggregate = context.repository.get(TestAggregate, context.stream_id)


@then("the aggregate is loaded from the snapshot and later events")
def step_impl(context):
    assert context.aggregate.version == 11
    assert context.aggregate.value == 1011
