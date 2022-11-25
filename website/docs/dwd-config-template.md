---
title: "DWD config template"
sidebar_position: 3
toc: true
last_modified_at: 2022-11-25T14:26:32-04:00
---

本片文档主要介绍DWD配置模板的参数和使用方式。

配置模板example可以参考quick start的[配置文件](https://docs.google.com/spreadsheets/d/1CetkqBsXj_E8oZBsws9iGdaJB1QJUajnwqH4FoKhXKA/edit#gid=1485376124)。

## 数据源配置：dwd_etl_config

`source_connection`: 配置在application.properties中的connection

`source_type`: 数据库类型，例如：hive

`source_db`: 从哪个数据库获取数据

`source_table`: 从哪张表获取数据

`target_connection`: 目标连接，配置在application.properties中的connection。例如：hive

`target_type`: 目标数据库类型，例如：hive

`target_db`: dwd数据库库名

`target_table`: dwd表名

`fact_or_dim`: 标识当前表是维度表还是事实表。当配置为dim（维度表）时，如果`slow_changing`未配置，默认`slow_changing`的值为TRUE；当配置为fact（事实表）时，如果`slow_changing`未配置，默认`slow_changing`的值为FALSE；【枚举值：fact/dim】

`slow_changing`: 标识当前表是否渐变。【枚举值：TRUE/FALSE】

`row_filter_expression`: 是否可空：是。例如：location = 'shanghai'，表示只取上海地区的数据。会作为where表达式拼接在查询源数据表的sql中

`load_type`: 标识增量全量【枚举值：incremental/full】

`log_driven_type`: 日志驱动类型，【枚举值：timewindow/upstream/kafka_offset/auto_inc_id/diff】其中，timewindow为基于时间窗口；upstream为基于上一层ETL依赖；kafka_offset为基于消息队列offset；auto_inc_id为基于自增id（该id必须代表数据的唯一性）；diff为基于文件对比

`upstream`: 日志驱动类型为upstream模式下的相关配置，标识依赖于哪一个ETL job

`depends_on`: 调度服务依赖。

`default_start`: timewindow模式下的开始时间


## 表配置：dwd_config

`source_table`: 从哪张表获取数据

`target_table`: 当前ETL的目标表名称

`source_column`: 源表列名称

`source_column_description`: 列描述

`target_column`: 目标表需要保留的列名

`extra_column_expression`: 对于`source_column`不存在，但是`target_column`存在的列的逻辑表达式描述。其中**zip_id_flag**值为特殊标识，代表该`target_column`为关联主数据后获取的主数据zip_id

`partition_column`: 标识哪些列作为分区列。用TRUE标识

`logic_primary_column`: 标识逻辑主键。用TRUE标识

`join_db_connection`: 标识需要关联的维度表所在数据库的连接，配置在application.properties中的connection

`join_db_type`: 标识需要关联的维度表所在数据库的类型

`join_db`: 标识需要关联的维度表所在数据库的名称

`join_table`: 标识需要关联的维度表名

`join_on`: 标识和需要关联的维度表的哪个列进行关联

`create_dim_mode`: 标识当前表中的维度信息是否要插入到维度表的标识。【枚举值：never/once/always】解释如下。

* never：当关联不上维度中的维度时，当前表的该列标识为关联不上维度信息，默认为-1

* once：当关联不上维度中的维度时，会从当前表中往维度表中插入缺失的维度信息，通常为迟到维的处理方法

* always：从当前表中往维度表中插入维度信息，即，当前表会作为维度表的一个数据源头

`join_table_column`: 标识需要关联的维度表的列名。通常用作`create_dim_mode`为once和always的场景，即当前表的哪些列需要插入到所关联的维度表中

`business_create_time`: 标识当前表的业务时间

`business_update_time`:  标识当前表的更新时间

`ignore_changing_column`: 标识当前表的哪些列不用作为拉链表的渐变列。用TRUE标识。该列为拉链表的标识列