---
title: "transformer guide"
sidebar_position: 10
toc: true
last_modified_at: 2021-12-23T18:25:57-04:00
---

## transformer的定义和使用

`transformer`是用于满足用户对于特定场景下通过代码逻辑实现的扩展诉求，通过反射加载jar或者文本文件中的scala代码。`transformer`允许在job step中执行一段代码块，通过java class path或者文件名来区分不同的`transformer`，不同的`transformer`会有不同的自定义参数，如：

```sql
-- step=1
-- source=transformation
--  className=com.github.sharpdata.sharpetl.spark.transformation.HttpTransformer
--  methodName=transform
--  transformerType=object
--  url=https:xxxx
--  connectionName=connection_demo
--  fieldName=centerIds
--  jsonPath=$.centers[*].id
--  splitBy=,
-- target=variables
-- checkPoint=false
-- dateRangeInterval=0
```

以HttpTransformer为例，该transformer是将从api获得的json类型数据进行解析和落表，还支持url中的动态传参，可以采用variables定义具体参数并调用。在所有使用transformer的step中，`dataSourceType=transformation, methodName=transform, transformerType=object`，className则为定义transformer的类名，除此之外的`url, connectionName, fieldName, jsonPath, splitBy`则为httpTransformer的自定义参数。



### 自定义transformer的使用

- `JdbcResultSetTransformer`: 该transformer主要用于对source数据库中的表执行`insert`，`update`，`delete`等无返回值的操作

  ```sql
  -- step=1
  -- source=transformation
  --  className=com.github.sharpdata.sharpetl.spark.transformation.JdbcResultSetTransformer
  --  dbType=yellowbrick
  --  dbName=bigdata
  --  methodName=transform
  -- target=do_nothing
  -- checkPoint=false
  -- dateRangeInterval=0
  delete from demo_table where to_char("HIST_DT", 'yyyyMMdd') = '${TODAY}';
  ```

    - `dataSourceType`, `className`和`methodName`与前文保持一致
    - `dbType`和`dbName`用于构建jdbc连接参数，`dbType`为`jdbc transformer`的自定义参数
    - 该step表示从`demo_table`中删除当天的数据

- `DDLTransformer`: 该transformer主要用于通过建表语句在`hive`或者`yellobrick`建表，需要传入建表语句的路径

  ```sql
  -- step=1
  -- source=transformation
  --  className=com.github.sharpdata.sharpetl.spark.transformation.DDLTransformer
  --  methodName=transform
  --  transformerType=object
  --  dbName=bigdata
  --  dbType=yellowbrick
  --  ddlPath=/demo_ddl
  -- target=do_nothing
  ```

    - `dbType`&`dbName`: 用于构建jdbc连接，目前只支持通过ddl在hive和yb建表
    - `ddlPath`: 默认值为`/user/hive/sharp-etl/ddl`，为存放ddl的具体路径

- `JobDependencyCheckTransformer`: 该transformer主要用于对job运行时上游依赖job是否运行结束的检测，需要输入`decencies`和`jobName`，检查该jobName是否有依赖job未运行完成

  ```sql
  -- step=1
  -- source=transformation
  --  className=com.github.sharpdata.sharpetl.spark.transformation.JobDependencyCheckTransformer
  --  methodName=transform
  --  transformerType=object
  --  dependencies=job1,job2,job3
  --  jobName=test_dependency_demo
  -- target=do_nothing
  ```

    - `dependencies`: 上游依赖job的名称，`jobName`即为需要检测的job名

- `FileCleanTransformer`: 该transformer将删除目标路径下固定格式的文件，需要输入`filePath`和`fileNamePattern`，后者支持正则表达式

  ```sql
  -- step=1
  -- source=transformation
  --  className=com.github.sharpdata.sharpetl.spark.transformation.FileCleanTransformer
  --  methodName=transform
  --  transformerType=object
  --  filePath=test_fileClean
  --  fileNamePattern=((\w*_test_fileClean.txt))
  -- target=do_nothing
  ```

    - `filePath`: 文件存储路径
    - `fileNamePattern`: 文件名，也可传入正则表达式

- `DropExternalTableTransformer`: 该transformer将删除hive和hdfs中以`tableNamePrefix`开头的在`databaseName`数据库中相应`partition`的表和文件, `partition`支持动态传参，可以与`variables`结合使用

  ```sql
  -- step=2
  -- source=transformation
  --  className=com.github.sharpdata.sharpetl.spark.transformation.DropExternalTableTransformer
  --  methodName=transform
  --  transformerType=object
  --  databaseName=testDB
  --  tableNamePrefix=pre_ods__
  --  partition=year=${YEAR},month=${MONTH},day=${7_DAYS_BEFORE}
  -- target=do_nothing
  ```

    - `partition`: 需要删除的分区，会删除`day=${7_DAYS_BEFORE}`即7天前的分区文件
    - `tableNamePrefix`: 需要删除的表前缀

- `DailyJobsSummaryReportTransformer`: 该transformer主要用于发送附件为`dailyReportSummary`的csv邮件，包含当日所有的job，粒度为step，对于失败job，还会汇总具体的失败信息和具体step，主要依赖于`step_log`和`job_log`两张表，需要在配置文件中配置`projectName`和`jobName`（如果不配置，则`dailyReport`的csv文件中没有具体的`projectName`）

  ```sql
  -- step=1
  -- source=transformation
  --  className=com.github.sharpdata.sharpetl.spark.transformation.DailyJobsSummaryReportTransformer
  --  methodName=transform
  --  transformerType=object
  --  datasource=hive,yellowbrick
  -- target=do_nothing
  -- checkPoint=false
  -- dateRangeInterval=0
  ```

    - `datasource`: 写入的数据库类型，用于汇总具体的`errorMessage`

- `CheckAllConnectorStatusTransformer`: 该transformer集成了`kafka restapi`，通过调用接口返回各个`connector`的运行状态，若有`connector`状态为`failed`或`pause`，则会及时预警

  ```sql
  -- step=1
  -- source=transformation
  --  className=com.github.sharpdata.sharpetl.spark.transformation.CheckAllConnectorStatusTransformer
  --  methodName=transform
  --  transformerType=object
  --  uri=https://xxx.com:28085
  -- target=do_nothing
  ```

    - `uri`: 为具体的`kafka restapi`端口信息，会在代码中拼接真正需要访问的uri信息

- `CheckConnectorStatusTransformer`: 该transformer与`CheckAllConnectorStatusTransformer`一致，均用做monitor，需要指定具体`connector`的名字，与`connectorName`参数联合使用，可以一次输入一个或多个`connector`，通常用在kafka下游任务中的step1，即首先判断`connector`是否正常工作，若`connector`报错则下游任务不会执行，需要在配置文件中配置`kafka.restapi`参数，用于构建具体api

  ```sql
  -- step=1
  -- source=transformation
  --  className=com.github.sharpdata.sharpetl.spark.transformation.CheckConnectorStatusTransformer
  --  methodName=transform
  --  transformerType=object
  --  connectorName=connector1, connector2
  -- target=do_nothing
  ```

    - `connectorName`: 需要monitor的`connector`名字，通常为接入表时的`source connector`和`sink connector`

- `EnsureSinkConnectorFinished`: 该transformer用于确认`sink connector`即`consumer`端是否消费完数据，是否将数据全部写入hive/hdfs，一般接在`CheckConnectorStatusTransformer`后，为step2，若`consumer`还未写入完成，会等待5分钟，若5分钟后还未完成，则下游任务不会执行

  ```sql
  -- step=2
  -- source=transformation
  --  className=com.github.sharpdata.sharpetl.spark.transformation.EnsureSinkConnectorFinished
  --  methodName=transform
  --  transformerType=object
  --  group=consumer-group1
  --  kafkaTopic=topic1
  -- target=do_nothing
  ```

    - `group`: 为`kafka consumer group`名称，通常一个`group`对应一个`topic`，若一个`consumer group`包含多个`topic`，需要指定`kafkaTopic`名称
    - `kafkaTopic`: `topic`名称，若不指定，则默认为检查`group`里全部`topic`中的`message`是否全被消费



### 如何自定义transformer

可以在`com.github.sharpdata.sharpetl.spark.transformation`包中定制`transformer`，通过`override transform`方法实现具体逻辑，而`transformer`的调用则主要通过反射进行，只需在sql脚本中指定具体的`transformer`名称和相应参数即可。

### 加载外部transformer

框架还支持动态加载scala脚本文件，一个示例如下：

```scala
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.jayway.jsonpath.{JsonPath, PathNotFoundException}
import com.github.sharpdata.sharpetl.core.util.ETLLogger
import com.github.sharpdata.sharpetl.spark.common.ETLSparkSession
import com.github.sharpdata.sharpetl.spark.transformation._
import com.github.sharpdata.sharpetl.spark.utils.HttpStatusUtils
import net.minidev.json.JSONArray
import org.apache.http.impl.client._
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object LoopHttpTransformer extends Transformer {

  val mapper = new ObjectMapper with ScalaObjectMapper
  mapper.setSerializationInclusion(Include.NON_ABSENT)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  mapper.registerModule(DefaultScalaModule)

  override def transform(args: scala.collection.mutable.Map[String, String]): DataFrame = {
    ???
  }
}
```

### Pro tips

:::tip
编写scala脚本实现的transformer，需要注意几个要点：

* 文件开头不可带有package信息，在sql中调用时，会根据package.fileName中的fileName找到scala脚本
* sql在引用scala脚本时需要设置`transformerType=dynamic_object`，除此之外使用方式与jar中的transformer相同
* 部分package需使用全名例如 `scala.collection.mutable.Map[String, String]`, 而不是 `mutable.Map[String, String]`
* 如果你遇到了类似于`illegal cyclic reference involving object InterfaceAudience`的错误，你需要spark-submit option `--conf  "spark.executor.userClassPathFirst=true" --conf  "spark.driver.userClassPathFirst=true"`
* 如果你遇到了错误`object x is not a member of package x`，你需要使用全引用例如 `scala.collection.mutable.Map[String, String]`
:::
