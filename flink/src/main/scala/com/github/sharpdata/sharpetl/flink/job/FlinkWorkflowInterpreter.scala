package com.github.sharpdata.sharpetl.flink.job

import com.github.sharpdata.sharpetl.core.api.{Variables, WorkflowInterpreter}
import com.github.sharpdata.sharpetl.core.datasource.config.{DataSourceConfig, FileDataSourceConfig, StreamingDataSourceConfig}
import com.github.sharpdata.sharpetl.core.exception.Exception.{EmptyDataException, FileDataSourceConfigErrorException, IncrementalDiffModeTooMuchDataException}
import com.github.sharpdata.sharpetl.core.quality.QualityCheckRule
import com.github.sharpdata.sharpetl.core.repository.QualityCheckAccessor
import com.github.sharpdata.sharpetl.core.repository.model.JobLog
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.core.util.Constants._
import com.github.sharpdata.sharpetl.core.util.DateUtil.{BigIntToLocalDateTime, L_YYYY_MM_DD_HH_MM_SS}
import com.github.sharpdata.sharpetl.core.util.ETLConfig.{incrementalDiffModeDataLimit, partitionColumn}
import com.github.sharpdata.sharpetl.core.util.StringUtil.{BigIntConverter, isNullOrEmpty}
import com.github.sharpdata.sharpetl.core.util.{ETLLogger, HDFSUtil}
import com.github.sharpdata.sharpetl.flink.job.Types.DataFrame
import com.github.sharpdata.sharpetl.flink.quality.FlinkQualityCheck
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment

import scala.collection.convert.ImplicitConversions._
import scala.collection.immutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

class FlinkWorkflowInterpreter(override val tEnv: StreamTableEnvironment,
                               override val dataQualityCheckRules: Map[String, QualityCheckRule],
                               override val qualityCheckAccessor: QualityCheckAccessor)
  extends FlinkQualityCheck(tEnv, dataQualityCheckRules, qualityCheckAccessor) with WorkflowInterpreter[DataFrame] {


  override def evalSteps(steps: List[WorkflowStep], jobLog: JobLog, variables: Variables, start: String, end: String): Unit = {
    val batchStepNum = countBatchStepNum(steps)
    if (batchStepNum > 0) {
      super.evalSteps(steps, jobLog, variables, start, end)
      cleanUpTempTableFromMemory()
    }
//    if (batchStepNum < steps.length) {
//      executeMicroBatchSteps(steps.slice(batchStepNum, steps.length), jobLog, variables, start, end)
//    }
  }

  private def cleanUpTempTableFromMemory(): Unit = {
    if (Environment.CURRENT != "test") {
      tEnv.listTemporaryTables().foreach(it => tEnv.dropTemporaryTable(it))
    }
  }

  def countBatchStepNum(steps: List[WorkflowStep]): Int = {
    var batchStepNum = 0
    while (steps.length > batchStepNum &&
      !steps(batchStepNum).getSourceConfig.isInstanceOf[StreamingDataSourceConfig]) {
      batchStepNum += 1
    }
    batchStepNum
  }

  override def listFiles(step: WorkflowStep): List[String] = {
    val conf = step.source.asInstanceOf[FileDataSourceConfig]

    val files: List[String] = if (!isNullOrEmpty(conf.filePaths)) {
      conf.filePaths.split(",").toList
    } else {
      conf.dataSourceType match {
        case DataSourceType.FTP => ???
//          val sourceConfig = step.getSourceConfig[FileDataSourceConfig]
//          val ftpConfig = new FtpConnection(sourceConfig.getConfigPrefix)
//          if (sourceConfig.getFileDir != null && sourceConfig.getFileDir != "") {
//            ftpConfig.dir = sourceConfig.getFileDir
//          }
//          listFileUrl(
//            ftpConfig,
//            sourceConfig.getFileNamePattern
//          )
//        case DataSourceType.HDFS | DataSourceType.JSON |
//             DataSourceType.EXCEL | DataSourceType.CSV =>
//          HdfsDataSource.listFileUrl(step)
//        case DataSourceType.SCP =>
//          ScpDataSource.listFilePath(step)
        case _ =>
          throw FileDataSourceConfigErrorException(s"Not supported data source type ${conf.dataSourceType}")
      }
    }

    ETLLogger.info(s"Files will to be processed:\n ${files.mkString(",\n")}")
    files
  }

  override def deleteSource(step: WorkflowStep): Unit = {
    val sourceConfig = step.getSourceConfig[FileDataSourceConfig]
    val configPrefix = sourceConfig.getConfigPrefix
    if (sourceConfig.getDeleteSource.toBoolean) {
      val sourceFilePath = sourceConfig.getFilePath
      step.getSourceConfig[DataSourceConfig].getDataSourceType.toLowerCase match {
        case DataSourceType.FTP =>
//          FtpDataSource.delete(configPrefix, sourceFilePath)
        case DataSourceType.HDFS =>
          HDFSUtil.delete(sourceFilePath)
        case DataSourceType.SCP =>
//          ScpDataSource.delete(configPrefix, sourceFilePath)
        case _ =>
      }
    }
  }

  override def readFile(step: WorkflowStep,
                        jobLog: JobLog,
                        variables: Variables,
                        files: List[String]): DataFrame = {
    val df: DataFrame = if (files.isEmpty) {
      null // scalastyle:ignore
    } else {
//      val dfs = files
//        .map(file => {
//          step.getSourceConfig[FileDataSourceConfig].setFilePath(file)
//          executeRead(step, jobLog, variables)
//        }).filter(df => !df.isEmpty)
//
//      if (dfs.nonEmpty) {
//        dfs.reduce(_ unionByName _)
//      } else {
//        null // scalastyle:ignore
//      }
      ???
    }
    if (step.skipFollowStepWhenEmpty == BooleanString.TRUE && (df == null)) {
      throw EmptyDataException("Job skipping, because `skipFollowStepWhenEmpty` is true and file is empty!", step.step)
    }
    df
  }

  override def executeWrite(jobLog: JobLog, df: DataFrame, step: WorkflowStep, variables: Variables): Unit = {
    val stepLog = jobLog.getStepLog(step.step)
    val incrementalType = jobLog.logDrivenType
    ETLLogger.info(s"incremental type is ${incrementalType}")
    val dfCount = df.executeAndCollect().asScala.size
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
    stepLog.sourceCount = if (step.target.dataSourceType == DataSourceType.VARIABLES) 0 else df.executeAndCollect().asScala.size
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

  override def applicationId(): String = "???"//tEnv.getConfig.get("pipeline.name")

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
