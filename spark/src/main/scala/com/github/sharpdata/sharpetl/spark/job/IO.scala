package com.github.sharpdata.sharpetl.spark.job

import com.github.sharpdata.sharpetl.core.api.Variables
import com.github.sharpdata.sharpetl.core.datasource.{Sink, Source}
import com.github.sharpdata.sharpetl.core.datasource.config.DataSourceConfig
import com.github.sharpdata.sharpetl.core.exception.Exception.EmptyDataException
import com.github.sharpdata.sharpetl.core.repository.model.JobLog
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.core.util.Constants._
import com.github.sharpdata.sharpetl.datasource.kafka.DFConversations._
import com.github.sharpdata.sharpetl.core.annotation.AnnotationScanner
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

object IO {

  def read(spark: SparkSession,
           step: WorkflowStep,
           variables: Variables,
           jobLog: JobLog): DataFrame = {
    val dataSourceConfig = step.getSourceConfig[DataSourceConfig]

    val value: Class[Source[_, _]] = AnnotationScanner.sourceRegister(dataSourceConfig.dataSourceType)
    assert(value != null)

    val df = value.getMethod("read", classOf[WorkflowStep], classOf[JobLog], classOf[SparkSession], classOf[Variables])
      .invoke(value.newInstance(), step, jobLog, spark, variables)
      .asInstanceOf[DataFrame]

    addDerivedColumns(dataSourceConfig, df)
  }

  def write(df: DataFrame,
            step: WorkflowStep,
            variables: Variables): Unit = {
    val targetConfig = step.getTargetConfig[DataSourceConfig]
    if ((step.throwExceptionIfEmpty == BooleanString.TRUE || step.skipFollowStepWhenEmpty == BooleanString.TRUE)
      && df.isEmpty) {
      throw EmptyDataException(s"Job aborted, because step ${step.step} 's result is empty", step.step)
    }

    val value: Class[Sink[_]] = AnnotationScanner.sinkRegister(targetConfig.dataSourceType)
    assert(value != null)

    value.getMethod("sink", classOf[DataFrame], classOf[WorkflowStep], classOf[Variables])
      .invoke(value.newInstance(), df, step, variables)
  }

  private def addDerivedColumns(dataSourceConfig: DataSourceConfig, df: DataFrame): DataFrame = {
    if (dataSourceConfig.derivedColumns != null) {
      val derivedColumns = dataSourceConfig.derivedColumns
        .split(";")
        .map(_.split(":"))

      derivedColumns.foldLeft(df)((df: DataFrame, derivedColumn: Array[String]) =>
        df.withColumn(
          derivedColumn(0), lit(derivedColumn(1))
        )
      )
    } else {
      df
    }
  }
}
