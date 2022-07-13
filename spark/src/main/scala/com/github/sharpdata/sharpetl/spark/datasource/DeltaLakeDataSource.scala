package com.github.sharpdata.sharpetl.spark.datasource

import com.github.sharpdata.sharpetl.core.api.Variables
import com.github.sharpdata.sharpetl.core.datasource.{Sink, Source}
import com.google.common.base.Strings.isNullOrEmpty
import com.github.sharpdata.sharpetl.core.datasource.config.DBDataSourceConfig
import com.github.sharpdata.sharpetl.core.repository.model.JobLog
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.core.util.ETLConfig.deltaLakeBasePath
import com.github.sharpdata.sharpetl.core.annotation._
import com.github.sharpdata.sharpetl.datasource.kafka.DFConversations._
import com.github.sharpdata.sharpetl.core.util.ETLLogger
import org.apache.spark.sql.{DataFrame, SparkSession}

@source(types = Array("delta_lake"))
@sink(types = Array("delta_lake"))
class DeltaLakeDataSource extends Source[DataFrame, SparkSession] with Sink[DataFrame] {

  override def read(step: WorkflowStep, jobLog: JobLog, executionContext: SparkSession, variables: Variables): DataFrame = {
    load(executionContext, step)
  }

  override def sink(df: DataFrame, step: WorkflowStep, variables: Variables): Unit = {
    save(df, step)
  }

  def load(spark: SparkSession,
           step: WorkflowStep): DataFrame = {
    spark
      .read
      .format("delta")
      .load(s"$deltaLakeBasePath/${step.source.asInstanceOf[DBDataSourceConfig].tableName}")
  }

  def save(df: DataFrame, step: WorkflowStep): Unit = {
    val targetPath = s"$deltaLakeBasePath/${step.target.asInstanceOf[DBDataSourceConfig].tableName}"
    val isEmpty = df.isEmpty
    if (!isEmpty) {
      if (!isNullOrEmpty(step.target.asInstanceOf[DBDataSourceConfig].partitionColumn)) {
        writeByPartition(df, step, targetPath, step.target.asInstanceOf[DBDataSourceConfig].partitionColumn)
      }
      else {
        write(df, step, targetPath)
      }
    }
    else {
      ETLLogger.warn(s"No record in the result set. Not writing to Delta Lake Path:$targetPath")
    }
  }


  private def write(df: DataFrame, step: WorkflowStep, targetPath: String) = {
    df
      .write
      .format("delta")
      .mode(step.getWriteMode)
      .save(targetPath)
  }

  private def writeByPartition(df: DataFrame, step: WorkflowStep, targetPath: String, partitionColumn: String) = {
    df
      .write
      .format("delta")
      .mode(step.getWriteMode)
      .partitionBy(partitionColumn)
      .save(targetPath)
  }
}
