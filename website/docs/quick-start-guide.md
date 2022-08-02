---
title: "Quick Start Guide"
sidebar_position: 2
toc: true
last_modified_at: 2021-10-21T10:59:57-04:00
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This guide provides a quick peek to Sharp ETL's capabilities.
For a video guide, please check:

<iframe src="https://drive.google.com/file/d/1xaugrGkmzVPtJOzbTKObZCIvZfzyjgzj/preview" width="640" height="480" allow="autoplay"></iframe>

## Setup

Sharp ETL works well with Spark-2.3+ & Spark-3.2.+ versions. You can follow the instructions [here](https://github.com/SharpData/SharpETL/blob/master/.github/workflows/build.yml#L15-L19) for supported spark version.

<Tabs
defaultValue="bash"
values={[
{ label: 'bash', value: 'bash', }
]}>
<TabItem value="bash">

Build from source for your spark version:

```scala
//for spark 3.1 with scala 2.12
./gradlew buildJars -PscalaVersion=2.12 -PsparkVersion=3.1.2 -PscalaCompt=2.12.15
  
//for spark 2.4 with scala 2.12
./gradlew buildJars -PscalaVersion=2.12 -PsparkVersion=2.4.8 -PscalaCompt=2.12.15
  
//for spark 2.4 with scala 2.11
./gradlew buildJars -PscalaVersion=2.11 -PsparkVersion=2.4.8 -PscalaCompt=2.11.12
```

</TabItem>
</Tabs>

:::note Please note the following
<ul>
  <li>Spark only support JDK 1.8 before spark version 3.x</li>
  <li>Starting from spark version 3.x, JDK 11 support added, but <a href="https://issues.apache.org/jira/browse/SPARK-35557">support of JDK 17 is still missing</a></li>
  <li>For different versions of hive support, please follow <a href="https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html#interacting-with-different-versions-of-hive-metastore">Interacting with Different Versions of Hive Metastore</a></li>
</ul>
:::

Start a postgres instance

```bash
docker run --name postgres -e POSTGRES_PASSWORD=postgres -d -p 5432:5432 postgres:12.0-alpine
```

Start a ETL db instance

```bash
docker run --name mysql8 -d -p 3306:3306 -e MYSQL_ROOT_PASSWORD=root -e MYSQL_DATABASE=sharp_etl mysql:8.0
```

Suppose we have a table named `online_order` in postgres with schema `sales`:

<Tabs
defaultValue="sql"
values={[
{ label: 'SQL', value: 'sql', },
]}>
<TabItem value="sql">

```sql
-- This extension provides a function to generate a version 4 UUID, we must enable the extension first.
CREATE EXTENSION IF NOT EXISTS "pgcrypto";
create schema sales;
create table sales.online_order
(
    order_no           varchar(64) default gen_random_uuid() not null
        primary key,
    user_id            varchar(32)                           not null,
    user_name          varchar(32)                           not null,
    order_total_amount numeric,
    actual_amount      numeric,
    post_amount        numeric,
    order_pay_amount   numeric,
    total_discount     numeric,
    pay_type           varchar(32),
    source_type        varchar(32),
    order_status       varchar(32),
    note               varchar(32),
    confirm_status     varchar(32),
    payment_time       timestamp,
    delivery_time      timestamp,
    receive_time       timestamp,
    comment_time       timestamp,
    delivery_company   varchar(32),
    delivery_code      varchar(32),
    business_date      date        default CURRENT_DATE,
    return_flag        varchar(32),
    created_at         timestamp   default CURRENT_TIMESTAMP,
    updated_at         timestamp   default CURRENT_TIMESTAMP,
    deleted_at         timestamp
);
```

</TabItem>
</Tabs>

**We can download [this excel](https://docs.google.com/spreadsheets/d/1k4U2QgZyknJLfpJvVxASsiOcX2nIHX0tx_rUKAINLTY/edit#gid=0) to your `~/Desktop` for the quick start guide.**

:::tip
You can also use the existing [excel template](https://docs.google.com/spreadsheets/d/1eRgSHWKDaRufvPJLp9QhcnWiVKzRegQ6PeZocvAgHEo/edit#gid=0) for your new cases.
:::

## Generate sql files from excel config

<Tabs
defaultValue="bash"
values={[
{ label: 'bash', value: 'bash', },
]}>
<TabItem value="bash">

```bash
./gradlew :spark:run --args="generate-ods-sql -f ~/Desktop/sharp-etl-Quick-Start-Guide.xlsx --output ~/Desktop/"
```

</TabItem>

</Tabs>

And you can see a new file generated at `~/Desktop/sales.online_order.sql`

```sql
-- workflow=ods__t_fact_online_order
--  period=1440
--  loadType=incremental
--  logDrivenType=timewindow

-- step=1
-- source=postgres
--  dbName=postgres
--  tableName=sales.online_order
-- target=hive
--  dbName=ods
--  tableName=t_fact_online_order
-- writeMode=append
SELECT "order_no" AS "order_no",
       "user_id" AS "user_id",
       "user_name" AS "user_name",
       "order_total_amount" AS "order_total_amount",
       "actual_amount" AS "actual_amount",
       "post_amount" AS "post_amount",
       "order_pay_amount" AS "order_pay_amount",
       "total_discount" AS "total_discount",
       "pay_type" AS "pay_type",
       "source_type" AS "source_type",
       "order_status" AS "order_status",
       "note" AS "note",
       "confirm_status" AS "confirm_status",
       "payment_time" AS "payment_time",
       "delivery_time" AS "delivery_time",
       "receive_time" AS "receive_time",
       "comment_time" AS "comment_time",
       "delivery_company" AS "delivery_company",
       "delivery_code" AS "delivery_code",
       "business_date" AS "business_date",
       "return_flag" AS "return_flag",
       "created_at" AS "created_at",
       "updated_at" AS "updated_at",
       "deleted_at" AS "deleted_at",
       ${JOB_ID} AS "job_id",
       to_char("business_date", 'yyyy') as "year",
       to_char("business_date", 'MM') as "month",
       to_char("business_date", 'DD') as "day"
FROM "postgres"."sales"."online_order"
WHERE "business_date" >= '${DATA_RANGE_START}' AND "business_date" < '${DATA_RANGE_END}';
```

## Create ODS table

<Tabs
defaultValue="sql"
values={[
{ label: 'SQL', value: 'sql', },
]}>
<TabItem value="sql">

```sql
create schema ods;
create table ods.t_fact_online_order
(
    order_no           varchar(64) not null,
    user_id            varchar(32) not null,
    user_name          varchar(32) not null,
    order_total_amount numeric,
    actual_amount      numeric,
    post_amount        numeric,
    order_pay_amount   numeric,
    total_discount     numeric,
    pay_type           varchar(32),
    source_type        varchar(32),
    order_status       varchar(32),
    note               varchar(32),
    confirm_status     varchar(32),
    payment_time       timestamp,
    delivery_time      timestamp,
    receive_time       timestamp,
    comment_time       timestamp,
    delivery_company   varchar(32),
    delivery_code      varchar(32),
    business_date      date      default CURRENT_DATE,
    return_flag        varchar(32),
    created_at         timestamp default CURRENT_TIMESTAMP,
    updated_at         timestamp default CURRENT_TIMESTAMP,
    deleted_at         timestamp,
    job_id             varchar(16)
);
```
</TabItem>
</Tabs>

## Insert data

<Tabs
defaultValue="sql"
values={[
{ label: 'SQL', value: 'sql', },
]}>

<TabItem value="sql">

```sql
insert into sales.online_order(order_no, user_id, user_name, order_total_amount, actual_amount, post_amount,
                                order_pay_amount,
                                total_discount, pay_type, source_type, order_status, payment_time, business_date,
                                created_at, updated_at, deleted_at)
VALUES ('2021093000001', 1, '张三ð', 200.0, 100, 0, 99, 101.0, 'wechat', 'mini-program', 'paid', '2021-09-30 09:00:35',
        '2021-09-30',
        '2021-09-30 09:00:00', '2021-09-30 09:00:35', null),
        ('2021093000002', 2, '李四o(╥﹏╥)o', 399.0, 200, 0, 200, 199.0, 'wechat', 'official-website', 'paid',
        '2021-09-30 19:00:35',
        '2021-09-30',
        '2021-09-30 19:00:00', '2021-09-30 19:00:35', null);
```
</TabItem>

</Tabs>

## Before run job

The db connection infomation is specified in `application.properties` and Make sure your db connections are included in the file.  For a quick start, we need to add followings:

```
postgres.postgres.url=jdbc:postgresql://localhost:5432/postgres?stringtype=unspecified
postgres.postgres.user=postgres
postgres.postgres.password=postgres
postgres.postgres.driver=org.postgresql.Driver
postgres.postgres.fetchsize=10
```


## Run the job

Then we will run a sample job which reads data from `sales.online_order` table and write them into `ods.t_fact_online_order`

<Tabs
defaultValue="bash"
values={[
{ label: 'bash', value: 'bash', },
]}>
<TabItem value="bash">

```bash
# run single job by `spark-submit`
spark-submit --class com.github.sharpdata.sharpetl.spark.Entrypoint spark/build/libs/spark-1.0.0-SNAPSHOT.jar single-job --name=sales.online_order --period=1440 --default-start-time="2021-09-30 00:00:00" --local --once

# run single job locally
./gradlew :spark:run --args="single-job --name=sales.online_order --period=1440 --default-start-time='2021-09-30 00:00:00' --local --once"
```
:::note
You need to put sql file under `spark/src/main/resources/tasks` or put it into HDFS/DBFS to run `single-job`
If you want to configure the sql file folder, please set `etl.workflow.path` in `application.properties`
:::
</TabItem>
</Tabs>


## Query job result

<Tabs
defaultValue="sql"
values={[
{ label: 'SQL', value: 'sql', },
]}>
<TabItem value="sql">

```sql
SELECT * FROM ods.t_fact_online_order;
```

| order_no      | user_id | user_name    | order_total_amount | actual_amount | post_amount | order_pay_amount | total_discount | pay_type | source_type      | order_status | note | confirm_status | payment_time               | delivery_time | receive_time | comment_time | delivery_company | delivery_code | business_date | return_flag | created_at                 | updated_at                 | deleted_at | job_id |
| :------------ | :------ | :----------- | :----------------- | :------------ | :---------- | :--------------- | :------------- | :------- | :--------------- | :----------- | :--- | :------------- | :------------------------- | :------------ | :----------- | :----------- | :--------------- | :------------ | :------------ | :---------- | :------------------------- | :------------------------- | :--------- |:-------|
| 2021093000002 | 2       | 李四o(╥﹏╥)o | 399                | 200           | 0           | 200              | 199            | wechat   | official-website | paid         |      |                | 2021-09-30 19:00:35.000000 |               |              |              |                  |               | 2021-09-30    |             | 2021-09-30 19:00:00.000000 | 2021-09-30 19:00:35.000000 |            | 2      |
| 2021093000001 | 1       | 张三ð        | 200                | 100           | 0           | 99               | 101            | wechat   | mini-program     | paid         |      |                | 2021-09-30 09:00:35.000000 |               |              |              |                  |               | 2021-09-30    |             | 2021-09-30 09:00:00.000000 | 2021-09-30 09:00:35.000000 |            | 2      |
|               |         |              |                    |               |             |                  |                |          |                  |              |      |                |                            |               |              |              |                  |               |               |             |                            |                            |            |        |
</TabItem>
</Tabs>
