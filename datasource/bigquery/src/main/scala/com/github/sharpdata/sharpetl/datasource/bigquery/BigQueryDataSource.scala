package com.github.sharpdata.sharpetl.datasource.bigquery

import com.github.sharpdata.sharpetl.core.api.Variables
import com.github.sharpdata.sharpetl.core.datasource.Source
import com.github.sharpdata.sharpetl.core.annotation._
import com.github.sharpdata.sharpetl.core.datasource.config.BigQueryDataSourceConfig
import com.github.sharpdata.sharpetl.core.repository.model.JobLog
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.core.util.ETLConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.jdk.CollectionConverters._

@source(types = Array("bigquery"))
class BigQueryDataSource extends Source[DataFrame, SparkSession] {

  override def read(step: WorkflowStep, jobLog: JobLog, executionContext: SparkSession, variables: Variables): DataFrame = {
    val bigQueryDataSourceConfig = step.source.asInstanceOf[BigQueryDataSourceConfig]
    val bigQueryConfig: Map[String, String] = bigqueryProps(bigQueryDataSourceConfig.getSystem)

    executionContext
      .read
      .format("com.google.cloud.spark.bigquery")
      .options(bigQueryConfig)
      .load(step.sql)
  }

  def bigqueryProps(system: String): Map[String, String] = {
    val prefix = s"bigquery.$system."
    ETLConfig.plainProperties.filterKeys(_.startsWith(prefix))
      .map { case (k, v) => (k.replaceFirst(prefix, ""), v) }
      .toMap
  }
}
