---
title: "ODS config template"
sidebar_position: 3
toc: true
last_modified_at: 2022-11-23T17:59:57-04:00
---

本片文档主要介绍ODS配置模板的参数和使用方式。

配置模板example可以参考quick start的[配置文件](https://docs.google.com/spreadsheets/d/1eRgSHWKDaRufvPJLp9QhcnWiVKzRegQ6PeZocvAgHEo/edit#gid=0)。

## 数据源配置：ods_etl_config

`source_connection`: 配置在application.properties中的connection

`source_table`: 从哪张表获取数据

`source_db`: 从哪个数据库获取数据

`source_type`: 数据库类型，例如：mysql

`target_connection`: 目标连接，配置在application.properties中的connection。例如：hive

`target_table`: ods表名

`target_db`: ods数据库库名

`target_type`: 目标数据库类型，例如：hive

`row_filter_expression`: 是否可空：是。例如：location = 'shanghai'，表示只取上海地区的数据。会作为where表达式拼接在查询源数据表的sql中

`load_type`: 增量全量，可选值：incremental，full

`log_driven_type`: 日志驱动类型，可选值：timewindow/upstream/kafka_offset/auto_inc_id/diff

`upstream`: 依赖于哪一个上游任务，对于ods任务而言，一般为空

`depends_on`: 依赖于哪一个任务，对于ods任务而言，一般为空

`default_start`: timewindow模式下的开始时间

`partition_format`: 分区格式，可选值：空字符串或者year/month/day

`time_format`: 时间格式，默认值：YYYY-MM-DD hh:mm:ss

`period`: 多少分钟运行一次任务，对于daily的任务应为1440


## 表配置：ods_config

`source_table`: 从哪张表获取数据

`source_column`: 源表列名称

`column_type`: 源表列类型

`column_description`: 源表列描述

`is_PK`: 源表是否主键

`is_nullable`: 源表是否可空

`incremental_column`: 增量列，一般为业务时间字段

`target_table`: 目标表名

`target_column`: 目标表列

`extra_column_expression`: 扩展列表达式，可以在源表多个列的基础上做sql表达式计算，例如 md5(concat_ws('', user_name, .. , user_address))，结果作为新列插入目标列，这时对应的源列为空值。