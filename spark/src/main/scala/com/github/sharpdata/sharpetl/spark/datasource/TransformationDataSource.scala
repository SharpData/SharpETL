package com.github.sharpdata.sharpetl.spark.datasource

import com.github.sharpdata.sharpetl.core.annotation._
import com.github.sharpdata.sharpetl.core.api.Variables
import com.github.sharpdata.sharpetl.core.datasource.config.TransformationDataSourceConfig
import com.github.sharpdata.sharpetl.core.datasource.{Sink, Source}
import com.github.sharpdata.sharpetl.core.repository.model.JobLog
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.spark.utils.ReflectUtil
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Timestamp
import java.time.Instant

@source(types = Array("transformation"))
@sink(types = Array("transformation"))
class TransformationDataSource extends Source[DataFrame, SparkSession] with Sink[DataFrame] {

  override def read(step: WorkflowStep, jobLog: JobLog, executionContext: SparkSession, variables: Variables): DataFrame = {
    val sourceConfig = step.getSourceConfig[TransformationDataSourceConfig]

    val start = "dataRangeStart" -> variables.getOrElse("${DATA_RANGE_START}", "")
    val end = "dataRangeEnd" -> variables.getOrElse("${DATA_RANGE_END}", "")
    val jobId = "jobId" -> variables.getOrElse("${JOB_ID}", "")
    val jobName = "workflowName" -> variables.getOrElse("${JOB_NAME}", "")
    val jobTime = "jobTime" -> Timestamp.from(Instant.now()).toString
    val sql = "sql" -> step.sql

    ReflectUtil
      .execute(
        sourceConfig.className,
        sourceConfig.methodName,
        sourceConfig.transformerType,
        sourceConfig.args + start + end + jobId + jobTime + sql + jobName)
      .asInstanceOf[DataFrame]
  }

  override def sink(df: DataFrame, step: WorkflowStep, variables: Variables): Unit = {
    val targetConfig = step.getTargetConfig[TransformationDataSourceConfig]

    ReflectUtil
      .execute(
        targetConfig.className,
        targetConfig.methodName,
        targetConfig.transformerType,
        df,
        step,
        targetConfig.args,
        variables ++ Map("sql" -> step.sql)
      )
  }
}
