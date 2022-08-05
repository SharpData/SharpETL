---
slug: sharp-etl-introduce-03-what-is-log-driven
title: Sharp ETL介绍(三):什么是日志驱动
tags: [sharp etl, log driven]
date: 2022-08-03T00:00:00+08:00
---

## 导言

本文将从为什么需要日志驱动和日志驱动如何解决问题展开来对日志驱动做详细介绍

## 常规的任务调度

![Log Driven](/assets/images/logdriven-1.svg)

对于一个daily的任务，理想情况下是每天都会成功，但是实际上肯定会遇到失败的场景，不同的调度引擎往往对于失败的case有不同的处理逻辑。
这里以忽略过去失败的任务，继续开启下一个调度周期为例，对比实例，跳过了2022-02-02的任务，继续运行 2022-02-03的任务。当发现任务出问题之后，需要手动补数据，手动重新运行2022-02-02的任务，把数据补上去。（这里如果有报表使用了2022-02-02的数据，那么报表的数据肯定是不准确的）

缺点：完全依赖于调度工具的任务历史记录，如果没有配置失败通知机制，需要一个个去看哪个任务挂掉了。如果允许失败任务之后的任务继续运行，可能会导致对顺序有要求的场景下出问题，比如 2月3日的数据在mysql做了upsert操作，2月2日的又做了一次（在修复失败之后），就会导致用旧的数据覆盖新的数据的问题。

**这里也不否认有些调度框架在适当的配置时也能解决上述问题，但是没有人能保证所有的调度框架都能解决上面的问题或者你的项目对于调度框架有自由选择的空间。**

## 日志驱动

所谓的“日志驱动”，其实和“断点续传”这个概念很像，只不过没有应用在下载文件上，而是应用到了数仓。日志驱动有几个核心要点：

* 自行记录任务运行历史，而不依赖与调度框架的功能。这样就做到了与不同调度框架解绑；
* 调度是有序的，上个周期任务失败了，不会跳过它运行下个周期的任务，每次调度还是会先执行之前失败的任务，直到它成功；

日志驱动也带来了几点好处：

* 可以解决重复调度的问题，当任务运行后发现有相同任务在运行或者已经运行过了，当前任务可以直接退出或者kill掉之前的任务
* 补数据操作更加容易实现且灵活而不容易出错
* 更加灵活的任务依赖配置（任务上下游不一定是同频率或者必须在一个dag里面）
* 更加灵活的调度起始设置，例如对于kafka offset和自增主键的支持
* 更加统一且容易的运维操作（不同的项目、不同的调度引擎，都可以基于日志驱动(的表)来进行运维操作）
* 可以记录更加详细的任务状态，比如读到多少条数据，写了多少条数据等等（特指结构化记录，而不是普通的执行日志），方便做统计查看
* 可以自行选择事务级别（单独一篇展开讲）
* 数据模型分层解耦的作用（单独一篇展开讲）

![Log Driven](/assets/images/logdriven-2.svg)

回到现实场景，任务失败的情况大致可以分为两种：

* 重试就可以成功（网络闪崩，排队超时等）
* 代码、环境有问题需要人工介入的

对于重试就可以成功的情况，往往在下一次调度就可以自动补上之前失败的任务的数据；（如果不想等到下一个周期，可以马上调度一次）

对于无法重试成功的情况，往往每次调度都会挂掉，但是只会尝试最开始的那天的任务，因为前置的任务没有成功，只是在每天重试 2022-02-02 的任务；
无法重试成功的任务，仍然需要人工介入，修复（环境、逻辑、上游数据等问题）之后，自动（按顺序）补上之前挂掉的任务的数据；


## 日志驱动的执行逻辑

![Log Driven](/assets/images/logdriven-3.svg)

## 待执行队列的具体计算逻辑

* [time-based](https://github.com/big-data-platform/sharp-etl/blob/master/core/src/main/scala/com/thoughtworks/bigdata/etl/core/LogDrivenJob.scala#L148-L161)
    
    包括 db的增量和全量、API的增量和全量、文件的增量和全量 等

    `dataRangeStart`取 ‘开始的时间’ option `--default-start` 和 ‘上次成功任务的’ `dataRangeEnd` 的最大值，
    频率 取 option `--period`，根据这两个值计算`dataRangeEnd`，简易计算逻辑：

    应该schedule的次数 = (‘当前时间’ - `dataRangeStart`) / `period` （取整）

    对于一个没有运行过的任务，假设`--default-start`为 20220101000000，`--period`（单位：分钟）是 1440，就是说从2022-01-01 00:00:00开始每天运行一次任务，如果当前时间是2022-01-05 14:00:00，这时就会schedule 4个任务（20220101000000-20220102000000, 20220102000000-20220103000000, 20220103000000-20220104000000, 20220104000000-20220105000000）。

    对于一个运行过的任务，之前已经设定过`--default-start`为 20220101000000，但是已经运行过一段时间，假设当前任务对应的`dataRangeEnd`的最大值为20220107000000，当前时间为2022-01-08 14:00:00，那么会schedule 1个任务（20220107000000-20220108000000）。

* [auto-incremental primary key](https://github.com/big-data-platform/sharp-etl/blob/master/core/src/main/scala/com/thoughtworks/bigdata/etl/core/LogDrivenJob.scala#L126-L146)
    
    数据库自增主键场景
    `dataRangeStart`取 ‘开始主键值’ option `--default-start`（默认：0） 和 ‘上次成功任务的’ `dataRangeEnd` 的最大值，
    `dataRangeEnd`取值 `max(primary key)`, 任务运行结束后会更新这次任务的实际取到的最大主键值到`dataRangeEnd`中。

* [kafka topic](https://github.com/big-data-platform/sharp-etl/blob/master/core/src/main/scala/com/thoughtworks/bigdata/etl/core/LogDrivenJob.scala#L76-L123)
    
    与自增主键类似，
    `dataRangeStart`取 ‘开始主键值’ option `--default-start`（默认：`earlist`） 和 ‘上次成功任务的’ `dataRangeEnd` 的最大值，
    `dataRangeEnd`取值 `latest`, 任务运行结束后会更新这次任务的实际取到的最大offset到`dataRangeEnd`中。


## 日志驱动的表设计

日志驱动目前的实现主要有两张表 `job_log`, `step_log`

```sql
create table job_log
(
    job_id           bigint auto_increment comment '任务id，主键' primary key,
    job_name         varchar(128) charset utf8          not null,
    job_period       int                                not null comment '周期',
    job_schedule_id  varchar(128) charset utf8          not null comment 'job_name + data_range_start 拼接的字符串',
    data_range_start varchar(1024) charset utf8         null,
    data_range_end   varchar(1024) charset utf8         null,
    job_start_time   datetime                           null comment '任务开始时间',
    job_end_time     datetime                           null comment '任务结束时间',
    status           varchar(32) charset utf8           not null comment '任务结果:  SUCCESS,FAILURE,RUNNING',
    create_time      datetime default CURRENT_TIMESTAMP not null comment '数据插入时间',
    last_update_time datetime default CURRENT_TIMESTAMP not null comment '数据更新时间',
    incremental_type varchar(16)                        null,
    current_file     varchar(1024) charset utf8         null comment '文件数据源会记录文件名',
    application_id   varchar(64) charset utf8           null comment '执行引擎的任务id，例如spark的applicationId',
    project_name     varchar(256) charset utf8          null
);

create table step_log
(
    job_id        bigint           not null,
    step_id       varchar(64)      not null,
    status        varchar(32)      not null,
    start_time    datetime         not null,
    end_time      datetime         not null,
    duration      int(11) unsigned not null,
    output        text             not null comment 'info日志',
    source_count  bigint           null,
    target_count  bigint           null,
    success_count bigint           null comment '执行成功条数',
    failure_count bigint           null comment '执行失败条数',
    error         varchar(10000)   null comment 'error日志',
    source_type   varchar(256)     null comment '数据源类型',
    target_type   varchar(256)     null comment '目标源类型',
    primary key (job_id, step_id)
);
```