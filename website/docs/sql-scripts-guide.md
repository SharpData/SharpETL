---
title: "SQL script quick start"
sidebar_position: 9
toc: true
last_modified_at: 2021-12-23T18:25:57-04:00
---

## sql脚本的使用

ETL通过sql文件来定义，ETL的流程通过sql脚本中的step来划分，每个step都包含输入(`sourceConfig`)输出(`targetConfig`)的配置。source、target通过`dataSourceType`支持一系列数据源。同时还支持通过`transformer`等方式来ETL扩展自定义逻辑。

### 原生sql的结构如下：

```sql
-- step=1
-- source=temp
-- target=variables
-- checkPoint=false
-- dateRangeInterval=0
select from_unixtime(unix_timestamp('${DATA_RANGE_START}', 'yyyy-MM-dd HH:mm:ss'), 'yyyy') as `YEAR`,
       from_unixtime(unix_timestamp('${DATA_RANGE_START}', 'yyyy-MM-dd HH:mm:ss'), 'MM')   as `MONTH`,
       from_unixtime(unix_timestamp('${DATA_RANGE_START}', 'yyyy-MM-dd HH:mm:ss'), 'dd')   as `DAY`;

-- step=2
-- source=hive
--  dbName=sql_demo_db
--  tableName=sql_demo
-- target=yellowbrick
--  dbName=sql_demo_yb
--  tableName=sql_demo
-- checkPoint=false
-- dateRangeInterval=0
-- writeMode=overwrite
select * from sql_demo_db.sql_demo where year = '${YEAR}' and month = '${MONTH}' and day = '${DAY}';
```

- step1中`DATA_RANGE_START`为执行etl job时传入的内置参数，除`DATA_RANGE_START`外还包括`DATA_RANGE_END`以及`job_id`等参数，这些参数可以在sql脚本中直接进行调用
- step1将`DATA_RANGE_START`参数的年月日分别提取出来，并赋值给变量`YEAR`, `MONTH`, `DAY`，这三个变量可以在后面的step中以`'${变量名}'`的形式进行调用
- step2则从`hive`中`sql_demo_db`的`sql_demo`表中读取出满足where子句的数据，并将其写入`yellowbrick`中的`sql_demo`表中

![image-sql-structure](/assets/images/sql-scripts.png)






各个参数的含义及使用：

- `sourceConfig`: 源系统相关信息，读取数据
    - `dataSourceType`: 无默认值，支持`hive`, `temp`, `jdbc类型`, `batch_kafka`, `transformation`等
        - `hive`: 通过 SparkSession （需启用 hive 支持）执行 hive sql 并返回 DataFrame 以供后续操作
        - `temp`: 直接通过 SparkSession （不需启用 hive 支持）执行 hive sql 并返回 DataFrame 以供后续操作
        - `jdbc类数据库`: 源数据库类型，需要配置基本连接信息，格式为`${dbName}.${dataSourceType}.*`,
        - `batch_kafka`: 支持从kafka topic读取数据，需要配置`kafka bootstrop server`信息，和`topics`参数, `schemaDDL`参数结合使用，前者表示kafka topic名称，后者为对应topic中的数据结构(`columnName columnType`)
        - `transformation`: 支持自定义transformer接入多种数据源，一般为`sourceConfig`中参数，且需要和`className`, `methodName`, `transfermerType`结合使用，不同transformer有自定义参数，需要结合使用
    - `dbName`: 无默认值，源数据库库名
    - `tableName`：无默认值，源表表名



- `targetConfig`: 目标数据库相关信息，写出数据

    - `dataSourceType`: 无默认值，支持`hive`,`  jdbc类型`, `variables`, `console`, `temp`,`do_nothing`等

        - `hive`: 该 step 的计算结果将写入到目标 hive 表，默认采用动态分区的方式。
        - `Jdbc`类数据库: 目标数据库类型，需要配置基本连接信息，格式为`${dbName}.${dataSourceType}.*`,

        - `variables`:将执行结果为只有一行（可以为多列）数据的 DataFrame中每个字段都设置为全局变量，可以在后续 step 的 sql 中直接引用

        - `console`: 将该step中的执行结果进行输出
        - `temp`: 该 step 的计算结果将注册为当前 SparkSession 生命周期内可用的内存临时表，可在后续 `targetConfig/dataSourceConfig` 类型为 `hive` 或 `temp` 的 step 中直接在 sql 中调用
        - `do_nothing`: 该step将不对读取出的数据做任何操作，通常用于执行`delete`,`update`等无返回值的sql语句



- `WriteMode`：数据写入方式，指从源系统向目标系统写入时的方式

    - `upsert`: update+insert，需要和`primaryKey`参数结合使用
    - `append`: 追加写入
    - `overwrite`: 覆盖写入

- `incrementalType`: 日志驱动所依赖的增量类型，默认为`timeBased`，即`step_log`和`job_log`中`dataRangeStart`和`dataRageEnd`为时间

    - `incremental_kafka_offset`: 指从`kafka topic`中读取数据，此时在表中记录和维护的是`kafka topic`内的`offset`值
    - `incremental_auto_inc_id`: 并非`timebased`，记录和维护一个`int`值



PS：
- 由于不同类型的数据库在sql语法上会有所区别，故每个step中的sql语句语法是取决于`sourceConfig`中的数据库类









