---
slug: sharp-etl-introduce-05-workflow-in-a-glance
title: Sharp ETL介绍(五):Workflow入门
tags: [sharp etl, workflow]
date: 2022-08-05T00:00:00+08:00
---

## 导言


本文将快速讲解workflow的基本用法，包括

* 变量
* 临时表
* 控制流 workflow_spec.sql
* step读写数据
* 数据源
* 扩展
 * UDF
 * Transformer
 * 自定义数据源

<!--truncate-->

## 变量

Sharp ETL对变量有丰富的支持，包括任务运行内建的变量和用户自定义变量。

内置变量包括

* `${DATA_RANGE_START}`
* `${DATA_RANGE_END}`
* `${JOB_ID}`
* `${JOB_NAME}`
* `${WORKFLOW_NAME}`

针对timewindow任务还包括

* `${YEAR}`
* `${MONTH}`
* `${DAY}`
* `${HOUR}`
* `${MINUTE}`

用户可以在任意step中新增或覆盖变量，声明结束后，后续step即可使用该变量，例如

```sql
-- step=1
-- source=temp
-- target=variables
select from_unixtime(unix_timestamp('${DATA_RANGE_START}', 'yyyy-MM-dd HH:mm:ss'), 'yyyy') as `YEAR`,
       from_unixtime(unix_timestamp('${DATA_RANGE_START}', 'yyyy-MM-dd HH:mm:ss'), 'MM')   as `MONTH`,
       from_unixtime(unix_timestamp('${DATA_RANGE_START}', 'yyyy-MM-dd HH:mm:ss'), 'dd')   as `DAY`,
       from_unixtime(unix_timestamp('${DATA_RANGE_START}', 'yyyy-MM-dd HH:mm:ss'), 'HH')   as `HOUR`,
       'temp_source' as `sources`,
       'temp_target' as `target`,
       'temp_end' as `end`
```

## 临时表

临时表是Sharp ETL能够将复杂任务拆分的基础，当前Spark的实现就是使用了Spark的临时表。一段复杂的逻辑可以拆分为输出到多个临时表来简化逻辑。对于从temp表读数据的step而言，source可以忽略掉，不写source默认认为从temp读取数据。

```sql
-- step=1
-- target=temp
--  tableName=temp_table
select 'SUCCESS' as `RESULT`;

-- step=2
-- target=console
select * from temp_table;
```

<!-- ## 控制流 TODO-->

## 数据源

每个step都有source和target两个配置，具体配置使用可以参考 [Datasource](/docs/datasource) 这一节来使用。同一个workflow里面datasource之间可以任意组合使用，没有严格限制，用户也可以很方便的自定义新的数据源。


## 扩展

Sharp ETL从设计之初就一直考虑让用户可以很方便的扩展功能，无论是在step设计、transformer设计、UDF、动态加载transformer脚本、自定义数据质量规则、自定义数据质量检查脚本都能够很好的支持用户实现自定义逻辑。未来还会支持 分支判断、循环、抛出异常、错误处理分支 等等控制流，使得Sharp ETL的workflow更加像一个编程语言，这样用户就可以完全依赖于SQL来实现所有的功能。

### UDF

用户可以通过build自己的jar包来实现UDF的支持，这个jar包不需要基于Sharp ETL来实现，甚至仅仅是普通的Scala function即可。

例如，用户需要注册一个新的UDF来实现自定义逻辑，只需要编写普通代码即可：

```scala
class TestUdfObj extends Serializable {
  def testUdf(value: String): String = {
    s"$value-proceed-by-udf"
  }
}
```

打包完成后需要将jar包与Sharp ETL的jar包一起提交，这样就可以很轻易的引用自己的UDF了。

```bash
spark-submit --class com.github.sharpdata.sharpetl.spark.Entrypoint spark/build/libs/spark-1.0.0-SNAPSHOT.jar /path/to/your-udf.jar ... ...
```


```sql
-- step=1
-- source=class
--  className=com.github.sharpdata.sharpetl.spark.end2end.TestUdfObj
-- target=udf
--  methodName=testUdf
--  udfName=test_udf

-- step=2
-- source=temp
-- target=temp
--  tableName=udf_result
select test_udf('input') as `result`;
```

### Transformer

Transformer的相关详细使用可以参考 [Transformer](/docs/transformer-guide)。

### 自定义数据源

自定义数据源的相关详细使用可以参考 [自定义数据源](/docs/custom-datasource-guide)。

### 配置项

Sharp ETL配置项、Spark conf配置、系统连接信息配置等可以参考 [Properties file config](/docs/properties-file-config)。