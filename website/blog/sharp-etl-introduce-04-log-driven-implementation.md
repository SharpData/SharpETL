---
slug: sharp-etl-introduce-04-log-driven-implementation
title: Sharp ETL介绍(四):日志驱动实现
tags: [sharp etl, log driven]
date: 2022-08-04T00:00:00+08:00
---

## 导言

本文将具体展开日志驱动的实现逻辑和细节

<!--truncate-->

## 日志驱动的执行逻辑

![Log Driven](/assets/images/logdriven-3.svg)

## 待执行队列的具体计算逻辑

* [time-based](https://github.com/SharpData/SharpETL/blob/97f303cbd1f40a29780551851f690c283bcb2061/core/src/main/scala/com/github/sharpdata/sharpetl/core/api/LogDrivenInterpreter.scala#L189)
    
    包括 db的增量和全量、API的增量和全量、文件的增量和全量 等

    `dataRangeStart`取 ‘开始的时间’ option `--default-start` 和 ‘上次成功任务的’ `dataRangeEnd` 的最大值，
    频率 取 option `--period`，根据这两个值计算`dataRangeEnd`，简易计算逻辑：

    应该schedule的次数 = (‘当前时间’ - `dataRangeStart`) / `period` （取整）

    对于一个没有运行过的任务，假设`--default-start`为 20220101000000，`--period`（单位：分钟）是 1440，就是说从2022-01-01 00:00:00开始每天运行一次任务，如果当前时间是2022-01-05 14:00:00，这时就会schedule 4个任务（20220101000000-20220102000000, 20220102000000-20220103000000, 20220103000000-20220104000000, 20220104000000-20220105000000）。

    对于一个运行过的任务，之前已经设定过`--default-start`为 20220101000000，但是已经运行过一段时间，假设当前任务对应的`dataRangeEnd`的最大值为20220107000000，当前时间为2022-01-08 14:00:00，那么会schedule 1个任务（20220107000000-20220108000000）。

* [auto-incremental primary key](https://github.com/SharpData/SharpETL/blob/97f303cbd1f40a29780551851f690c283bcb2061/core/src/main/scala/com/github/sharpdata/sharpetl/core/api/LogDrivenInterpreter.scala#L148)
    
    数据库自增主键场景
    `dataRangeStart`取 ‘开始主键值’ option `--default-start`（默认：0） 和 ‘上次成功任务的’ `dataRangeEnd` 的最大值，
    `dataRangeEnd`取值 `max(primary key)`, 任务运行结束后会更新这次任务的实际取到的最大主键值到`dataRangeEnd`中。

* [kafka topic](https://github.com/SharpData/SharpETL/blob/97f303cbd1f40a29780551851f690c283bcb2061/core/src/main/scala/com/github/sharpdata/sharpetl/core/api/LogDrivenInterpreter.scala#L95)
    
    与自增主键类似，
    `dataRangeStart`取 ‘开始主键值’ option `--default-start`（默认：`earlist`） 和 ‘上次成功任务的’ `dataRangeEnd` 的最大值，
    `dataRangeEnd`取值 `latest`, 任务运行结束后会更新这次任务的实际取到的最大offset到`dataRangeEnd`中。

* [upstream](https://github.com/SharpData/SharpETL/blob/97f303cbd1f40a29780551851f690c283bcb2061/core/src/main/scala/com/github/sharpdata/sharpetl/core/api/LogDrivenInterpreter.scala#L173)
    
    upstream方式驱动的任务有点特殊，它往往是贴源层(ODS)之后的层需要的，例如我们在明细层（DWD）创建一个新的任务，这个任务天然有依赖，它依赖于贴源层(ODS)的任务，这个时候我们不像针对当前任务多做配置，而是希望当前任务完全fellow贴源层(ODS)任务的配置，只要ODS任务跑过了，就会自动跑DWD的任务。
    这种方式还有一个额外的好处，如果ODS的任务需要重刷，如果DWD是timewindow的任务，则需要重新配置，但是如果DWD是upstream，则不需要配置，只要ODS重刷了，DWD也会跟着重刷，它们完全同频，大大降低了配置任务依赖的难度。


## 日志驱动的表设计

日志驱动目前的实现主要有两张表 `job_log`, `step_log`

```sql
create table job_log
(
    job_id           bigint auto_increment primary key,
    workflow_name    varchar(128) charset utf8 not null,
    `period`         int                                not null,
    job_name         varchar(128) charset utf8 not null,
    data_range_start varchar(128) charset utf8 null,
    data_range_end   varchar(128) charset utf8 null,
    job_start_time   datetime null,
    job_end_time     datetime null,
    status           varchar(32) charset utf8 not null comment 'job status: SUCCESS,FAILURE,RUNNING',
    create_time      datetime default CURRENT_TIMESTAMP not null comment 'log create time',
    last_update_time datetime default CURRENT_TIMESTAMP not null comment 'log update time',
    load_type        varchar(32) null,
    log_driven_type  varchar(32) null,
    file             text charset utf8 null,
    application_id   varchar(64) charset utf8 null,
    project_name     varchar(64) charset utf8 null,
    runtime_args     text charset utf8 null
) charset = utf8;

create table step_log
(
    job_id        bigint      not null,
    step_id       varchar(64) not null,
    status        varchar(32) not null,
    start_time    datetime    not null,
    end_time      datetime null,
    duration      int(11) unsigned not null,
    output        text        not null,
    source_count  bigint null,
    target_count  bigint null,
    success_count bigint null comment 'success data count',
    failure_count bigint null comment 'failure data count',
    error         text null,
    source_type   varchar(32) null,
    target_type   varchar(32) null,
    primary key (job_id, step_id)
) charset = utf8;

```