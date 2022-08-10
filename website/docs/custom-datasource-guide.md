---
title: "custom-datasource-guide"
sidebar_position: 10
toc: true
last_modified_at: 2021-12-23T18:25:57-04:00
---

## 自定义数据源

实现自定义数据源包括两个API：[Source](https://github.com/SharpData/SharpETL/blob/main/core/src/main/scala/com/github/sharpdata/sharpetl/core/datasource/Source.scala)和[Sink](https://github.com/SharpData/SharpETL/blob/main/core/src/main/scala/com/github/sharpdata/sharpetl/core/datasource/Sink.scala)。

```scala
trait Source[DataFrame, Context] extends Serializable {
  def read(step: WorkflowStep, jobLog: JobLog, executionContext: Context, variables: Variables): DataFrame
}

trait Sink[DataFrame] extends Serializable {
  def write(df: DataFrame, step: WorkflowStep, variables: Variables): Unit
}
```

:::tip
针对支持JDBC的数据源无需额外实现，只需提交任务时提供该数据源的JDBC driver jar和在properties文件中配置链接即可, 例如对于informix的JDBC支持需要配置:

```properties
sysmaster.informix.url=jdbc:informix-sqli://localhost:9088/sysmaster:INFORMIXSERVER=informix;DELIMIDENT=Y
sysmaster.informix.user=informix
sysmaster.informix.password=in4mix
sysmaster.informix.driver=com.informix.jdbc.IfxDriver
sysmaster.informix.fetchsize=100
```
:::

## 以DataBricks Delta为例扩展新的数据源

### 创建Config class

因为Delta可以看做是表结构，所以这里沿用了`DBDataSourceConfig`, 你也可以根据实际情况按需添加新的配置。


```scala
import com.github.sharpdata.sharpetl.core.annotation.configFor

@configFor(types = Array("delta_lake"))
class DeltaLakeDataSourceConfig extends DBDataSourceConfig {
  @BeanProperty
  var yourConfig: String = _
}
```

:::tip
注意这里使用了`@configFor`注册了新的数据源类型，注册过后便可以在workflow里面作为source或target的值使用。
:::

### 实现 Source&Sink API

这里为了方便阅读，代码经过一定程度的简化。

```scala
import com.github.sharpdata.sharpetl.core.api.Variables
import com.github.sharpdata.sharpetl.core.datasource.{Sink, Source}
import com.github.sharpdata.sharpetl.core.datasource.config.DBDataSourceConfig
import com.github.sharpdata.sharpetl.core.repository.model.JobLog
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.core.annotation._
import com.github.sharpdata.sharpetl.core.util.ETLLogger
import org.apache.spark.sql.{DataFrame, SparkSession}

@source(types = Array("delta_lake"))
@sink(types = Array("delta_lake"))
class DeltaLakeDataSource extends Source[DataFrame, SparkSession] with Sink[DataFrame] {

  override def read(step: WorkflowStep, jobLog: JobLog, executionContext: SparkSession, variables: Variables): DataFrame = {
    spark
      .read
      .format("delta")
      .load(s"$deltaLakeBasePath/${step.source.asInstanceOf[DBDataSourceConfig].tableName}")
  }

  override def write(df: DataFrame, step: WorkflowStep, variables: Variables): Unit = {
    df
      .write
      .format("delta")
      .mode(step.getWriteMode)
      .save(targetPath)
  }
}

```

:::tip
注意这里使用了`@source`和`@sink`与数据源类型做关联，一个DataSource可以同时关联多个数据类型（只要他们的实现是一致的），也可以只实现Source或者Sink API。
:::