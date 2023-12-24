package com.github.sharpdata.sharpetl.flink.job

import com.github.sharpdata.sharpetl.core.api.{Variables, WorkflowInterpreter}
import com.github.sharpdata.sharpetl.core.exception.Exception.IncrementalDiffModeTooMuchDataException
import com.github.sharpdata.sharpetl.core.quality.QualityCheckRule
import com.github.sharpdata.sharpetl.core.repository.QualityCheckAccessor
import com.github.sharpdata.sharpetl.core.repository.model.JobLog
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.core.util.Constants._
import com.github.sharpdata.sharpetl.core.util.DateUtil.{BigIntToLocalDateTime, L_YYYY_MM_DD_HH_MM_SS}
import com.github.sharpdata.sharpetl.core.util.ETLConfig.{incrementalDiffModeDataLimit, partitionColumn}
import com.github.sharpdata.sharpetl.core.util.ETLLogger
import com.github.sharpdata.sharpetl.core.util.StringUtil.BigIntConverter
import com.github.sharpdata.sharpetl.flink.job.Types.DataFrame
import com.github.sharpdata.sharpetl.flink.quality.FlinkQualityCheck
import com.github.sharpdata.sharpetl.flink.util.ETLFlinkSession
import org.apache.flink.table.api.TableEnvironment

import scala.collection.convert.ImplicitConversions._
import scala.collection.immutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

class FlinkWorkflowInterpreter(override val tEnv: TableEnvironment,
                               override val dataQualityCheckRules: Map[String, QualityCheckRule],
                               override val qualityCheckAccessor: QualityCheckAccessor)
  extends FlinkQualityCheck(tEnv, dataQualityCheckRules, qualityCheckAccessor) with WorkflowInterpreter[DataFrame] {


  override def evalSteps(steps: List[WorkflowStep], jobLog: JobLog, variables: Variables, start: String, end: String): Unit = {
    super.evalSteps(steps, jobLog, variables, start, end)
  }

  // deprecated method
  override def listFiles(step: WorkflowStep): List[String] = ???

  // deprecated method
  override def deleteSource(step: WorkflowStep): Unit = ???

  // deprecated method
  override def readFile(step: WorkflowStep,
                        jobLog: JobLog,
                        variables: Variables,
                        files: List[String]): DataFrame = ???

  override def executeWrite(jobLog: JobLog, df: DataFrame, step: WorkflowStep, variables: Variables): Unit = {
    val stepLog = jobLog.getStepLog(step.step)
    val incrementalType = jobLog.logDrivenType
    ETLLogger.info(s"incremental type is ${incrementalType}")
    val dfCount = df.execute().collect().asScala.size
    if (incrementalType == IncrementalType.DIFF && dfCount > incrementalDiffModeDataLimit.toLong) {
      throw IncrementalDiffModeTooMuchDataException(
        s"Incremental diff mode data limit is $incrementalDiffModeDataLimit, but current data count is ${dfCount}"
      )
    }
    if (incrementalType != IncrementalType.AUTO_INC_ID
      && incrementalType != IncrementalType.KAFKA_OFFSET
      && incrementalType != IncrementalType.UPSTREAM) {
      //`dataRangeStart` must be a datetime
      //value of partition column, we will use it later
      variables.put(s"$${$partitionColumn}", jobLog.dataRangeStart.asBigInt.asLocalDateTime().format(L_YYYY_MM_DD_HH_MM_SS))
    }
    if (df != null) {
      val count = if (step.target.dataSourceType == DataSourceType.VARIABLES) 0 else dfCount
      stepLog.targetCount = count
      ETLLogger.info("[Physical Plan]:")
      try {
        IO.write(df, step, variables)
        stepLog.successCount = count
        stepLog.failureCount = 0
      } catch {
        case e: Throwable =>
          stepLog.successCount = 0
          stepLog.failureCount = count
          throw e
      }
    }
  }

  // scalastyle:off
  override def executeRead(step: WorkflowStep,
                           jobLog: JobLog,
                           variables: Variables): DataFrame = {
    val stepLog = jobLog.getStepLog(step.step)
    val df = IO.read(tEnv, step, variables, jobLog)
    stepLog.sourceCount = if (step.target.dataSourceType == DataSourceType.VARIABLES) 0 else df.execute().collect().asScala.size
    df
  }
  // scalastyle:on


  /**
   * 释放资源
   */
  override def close(): Unit = {
    try {
      //ETLSparkSession.release(tEnv)
    } catch {
      case NonFatal(e) =>
        ETLLogger.error("Stop Spark session failed", e)
    }
  }

  override def applyConf(conf: Map[String, String]): Unit = {
    conf.foreach {
      case (key, value) =>
        ETLLogger.warn(s"Setting Flink conf $key=$value")
        tEnv.getConfig.set(key, value)
    }
  }

  override def applicationId(): String = ETLFlinkSession.wfName

  override def executeSqlToVariables(sql: String): List[Map[String, String]] = {
    val result = tEnv.sqlQuery(sql).execute()
    val schema = result.getResolvedSchema.getColumnNames
    val data: immutable.Seq[Map[String, String]] =
      result.collect().toList
        .map(it =>
          schema.asScala.map(col => (col, it.getField(col).toString)).toMap
        )
    data
      .map(
        it =>
          it.map {
            case (key, value) => ("${" + key + "}", value)
          }
      )
      .toList
  }

  override def union(left: DataFrame, right: DataFrame): DataFrame = {
    if (left != null && right != null) {
      left.union(right)
    } else if (left == null && right == null) {
      null // scalastyle:ignore
    } else if (left == null) {
      right
    } else {
      left
    }
  }
}
