---
title: "Properties file config"
sidebar_position: 8
toc: true
last_modified_at: 2021-12-23T18:25:57-04:00
---

## Sharp ETL config

### `etl.workflow.path`

从哪里查找workflow文件，默认从classpath中的`tasks`目录查找，可以指定外部路径，目前仅支持HDFS路径。

classpath例子：

```properties
etl.workflow.path=tasks
```

HDFS例子：

```properties
etl.workflow.path=HDFS:///etl/workflows
```

## Spark config

在properties中内置的默认配置如下:

```properties
spark.default.spark.sql.adaptive.enabled=true
spark.default.spark.sql.adaptive.logLevel=info
spark.default.spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
spark.default.spark.sql.adaptive.coalescePartitions.enabled=true
spark.default.spark.sql.adaptive.coalescePartitions.minPartitionNum=1
spark.default.spark.sql.adaptive.fetchShuffleBlocksInBatch=true
spark.default.spark.sql.adaptive.localShuffleReader.enabled=true
spark.default.spark.sql.adaptive.skewJoin.enabled=true
spark.default.spark.sql.adaptive.skewJoin.skewedPartitionFactor=5
spark.default.spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes=400m
spark.default.spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin=0.2
spark.default.spark.sql.autoBroadcastJoinThreshold=-1
spark.default.spark.sql.adaptive.shuffle.targetPostShuffleInputSize=134217728
spark.default.hive.exec.dynamic.partition=true
spark.default.hive.exec.dynamic.partition.mode=nonstrict
spark.default.spark.sql.sources.partitionOverwriteMode=dynamic
spark.default.spark.serializer=org.apache.spark.serializer.KryoSerializer
spark.default.spark.kryoserializer.buffer.max=128m
spark.default.spark.sql.crossJoin.enabled=true
spark.default.spark.driver.cores=1
spark.default.spark.driver.memory=1g
spark.default.spark.driver.memoryOverhead=1g
spark.default.spark.driver.maxResultSize=0
spark.default.spark.executor.cores=2
spark.default.spark.executor.memory=4g
spark.default.spark.executor.memoryOverhead=2g
spark.default.spark.dynamicAllocation.enabled=true
spark.default.spark.shuffle.service.enabled=true
spark.default.spark.dynamicAllocation.minExecutors=1
spark.default.spark.dynamicAllocation.maxExecutors=4
spark.default.spark.streaming.stopGracefullOnShutdown=true
spark.default.spark.streaming.backpressure.enable=true
spark.default.spark.streaming.kafka.maxRatePerPartition=100000
```

:::tip
内置的spark config对所有任务起效，可以通过`spark.your workflow name.spark.config=xxx`的方式来覆盖全局默认配置。
:::

:::tip
在workflow内可以针对某一个step配置spark config：

```sql
-- step=set config in conf
-- source=temp
-- target=temp
--  tableName=do_nothing_table
-- conf
--  spark.sql.shuffle.partitions=1
SELECT 'result';
```

或者直接使用spark sql：

```sql
-- step=3
-- source=temp
-- target=temp
--  tableName=do_nothing_table
SET spark.sql.hive.version=0.12.1;
```

:::

## Connection

针对不同的系统可以配置对应的connection，配置方式请参考对应的[Datasource](/docs/datasource)中的配置示例来使用。
配置结构为: `connection name.source/target type.config=value`


## Using external properties file

可以使用外部properties文件，当需要在不同环境运行任务，且可能properties配置中可能含有敏感信息时推荐使用。在启动任务时加入参数：

```
--property=hdfs:///etl/conf/prod.properties
```


## Override properties in command-line

针对需要调试或临时修改的任务，可以在运行任务时选择覆盖properties文件中的内容。在启动任务时加入参数，多个参数请使用逗号分隔:

```
--override=mysql.password=XXXX,foo=bar
```


## Properties config priority

在多个地方配置properties之后的优先级为(以spark conf为例)：


Workflow SQL `SET` syntax > Workflow `conf` > Command-line override > properties override > class path `application.properties` file

