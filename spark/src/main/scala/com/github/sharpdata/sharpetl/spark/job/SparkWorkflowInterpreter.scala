package com.github.sharpdata.sharpetl.spark.job

import com.github.sharpdata.sharpetl.core.api.{Variables, WorkflowInterpreter}
import com.github.sharpdata.sharpetl.core.datasource.config.{DataSourceConfig, FileDataSourceConfig, StreamingDataSourceConfig}
import com.github.sharpdata.sharpetl.core.exception.Exception.{EmptyDataException, FileDataSourceConfigErrorException, IncrementalDiffModeTooMuchDataException}
import com.github.sharpdata.sharpetl.datasource.kafka.DFConversations._
import com.github.sharpdata.sharpetl.spark.utils.{ConvertUtils, ETLSparkSession}
import com.github.sharpdata.sharpetl.core.quality.QualityCheckRule
import com.github.sharpdata.sharpetl.core.repository.JobLogAccessor.jobLogAccessor
import com.github.sharpdata.sharpetl.core.repository.QualityCheckAccessor
import com.github.sharpdata.sharpetl.core.repository.model.JobLog
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.core.util.Constants._
import com.github.sharpdata.sharpetl.core.util.DateUtil.{BigIntToLocalDateTime, L_YYYY_MM_DD_HH_MM_SS}
import com.github.sharpdata.sharpetl.core.util.ETLConfig.{incrementalDiffModeDataLimit, partitionColumn}
import com.github.sharpdata.sharpetl.core.util.StringUtil.{BigIntConverter, isNullOrEmpty}
import com.github.sharpdata.sharpetl.core.util.{ETLLogger, HDFSUtil}
import ETLSparkSession.sparkSession
import com.github.sharpdata.sharpetl.core.datasource.connection.FtpConnection
import com.github.sharpdata.sharpetl.spark.datasource.FtpDataSource.listFileUrl
import com.github.sharpdata.sharpetl.spark.datasource.{FtpDataSource, HdfsDataSource, ScpDataSource, StreamingDataSource}
import com.github.sharpdata.sharpetl.spark.quality.SparkQualityCheck
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.time.LocalDateTime
import scala.util.control.NonFatal

class SparkWorkflowInterpreter(override val spark: SparkSession,
                               override val dataQualityCheckRules: Map[String, QualityCheckRule],
                               override val qualityCheckAccessor: QualityCheckAccessor)
  extends SparkQualityCheck(spark, dataQualityCheckRules, qualityCheckAccessor) with WorkflowInterpreter[DataFrame] {

  override def executeJob(steps: List[WorkflowStep],
                          jobLog: JobLog,
                          variables: Variables,
                          start: String,
                          end: String): Unit = {
    val batchStepNum = countBatchStepNum(steps)
    if (batchStepNum > 0) {
      executeBatchSteps(steps.take(batchStepNum), jobLog, variables, start, end)
    }
    if (batchStepNum < steps.length) {
      executeMicroBatchSteps(steps.slice(batchStepNum, steps.length), jobLog, variables, start, end)
    }
  }

  def executeBatchSteps(batchSteps: List[WorkflowStep],
                        jobLog: JobLog,
                        variables: Variables,
                        start: String,
                        end: String): Unit = {
    executeSteps(batchSteps, jobLog, variables, start, end)
    cleanUpTempTableInMemory()
  }

  private def cleanUpTempTableInMemory(): Unit = {
    if (Environment.current != "test") {
      val tempTableNames = spark.catalog.listTables().filter(_.isTemporary)
      tempTableNames.collect().foreach(it => spark.catalog.dropTempView(it.name))
    }
  }

  def executeMicroBatchSteps(microBatchSteps: List[WorkflowStep],
                             jobLog: JobLog,
                             variables: Variables,
                             start: String,
                             end: String): Unit = {
    val firstMicroBatchStep = microBatchSteps.head

    val streamingDataSourceConfig = firstMicroBatchStep.getSourceConfig.asInstanceOf[StreamingDataSourceConfig]
    val streamingContext = new StreamingContext(
      sparkSession.sparkContext,
      Seconds(streamingDataSourceConfig.getInterval.toInt)
    )
    val stream = StreamingDataSource
      .createDStream(firstMicroBatchStep, jobLog, streamingContext)

    StreamingStep.executeStreamingStep(streamingDataSourceConfig, stream, (df: DataFrame) => {
      executeWrite(jobLog, df, firstMicroBatchStep, variables)
      executeSteps(microBatchSteps.tail, jobLog, variables, start, end)

      jobLog.setLastUpdateTime(LocalDateTime.now())
      jobLogAccessor.update(jobLog)
    })
    streamingContext.start()
    streamingContext.awaitTermination()
  }

  def countBatchStepNum(steps: List[WorkflowStep]): Int = {
    var batchStepNum = 0
    while (steps.length > batchStepNum &&
      !steps(batchStepNum).getSourceConfig.isInstanceOf[StreamingDataSourceConfig]) {
      batchStepNum += 1
    }
    batchStepNum
  }

  override def transformListFiles(filePaths: List[String]): DataFrame = {
    sparkSession.sql(s"SELECT '${filePaths.mkString(",")}' AS `FILE_PATHS`")
  }

  override def listFiles(steps: List[WorkflowStep], step: WorkflowStep): List[String] = {
    val conf = step.source.asInstanceOf[FileDataSourceConfig]

    val files: List[String] = if (!isNullOrEmpty(conf.filePaths)) {
      conf.filePaths.split(",").toList
    } else {
      conf.dataSourceType match {
        case DataSourceType.FTP =>
          val sourceConfig = step.getSourceConfig[FileDataSourceConfig]
          val ftpConfig = new FtpConnection(sourceConfig.getConfigPrefix)
          if (sourceConfig.getFileDir != null && sourceConfig.getFileDir != "") {
            ftpConfig.dir = sourceConfig.getFileDir
          }
          listFileUrl(
            ftpConfig,
            sourceConfig.getFileNamePattern
          )
        case DataSourceType.HDFS | DataSourceType.JSON |
             DataSourceType.EXCEL | DataSourceType.CSV =>
          HdfsDataSource.listFileUrl(step)
        case DataSourceType.SCP =>
          ScpDataSource.listFilePath(step)
        case _ =>
          throw FileDataSourceConfigErrorException(
            s"Not supported data source type ${conf.dataSourceType}"
          )
      }
    }

    ETLLogger.info(s"Files need to be processed:\n ${files.mkString(",\n")}")
    files
  }

  override def deleteSource(step: WorkflowStep): Unit = {
    val sourceConfig = step.getSourceConfig[FileDataSourceConfig]
    val configPrefix = sourceConfig.getConfigPrefix
    if (sourceConfig.getDeleteSource.toBoolean) {
      val sourceFilePath = sourceConfig.getFilePath
      step.getSourceConfig[DataSourceConfig].getDataSourceType.toLowerCase match {
        case DataSourceType.FTP =>
          FtpDataSource.delete(configPrefix, sourceFilePath)
        case DataSourceType.HDFS =>
          HDFSUtil.delete(sourceFilePath)
        case DataSourceType.SCP =>
          ScpDataSource.delete(configPrefix, sourceFilePath)
        case _ =>
      }
    }
  }

  override def readFile(step: WorkflowStep,
                        jobLog: JobLog,
                        variables: Variables,
                        files: List[String]): DataFrame = {
    val df = if (files.isEmpty) {
      null // scalastyle:ignore
    } else {
      val dfs = files
        .map(file => {
          step.getSourceConfig[FileDataSourceConfig].setFilePath(file)
          executeRead(step, jobLog, variables)
        }).filter(df => !df.isEmpty)

      if (dfs.nonEmpty) {
        dfs.reduce(_ unionByName _)
      } else {
        null // scalastyle:ignore
      }
    }
    if (step.skipFollowStepWhenEmpty == BooleanString.TRUE && (df == null || df.isEmpty)) {
      throw EmptyDataException("Job skipping, because `skipFollowStepWhenEmpty` is true and file is empty!", step.step)
    }
    df
  }

  override def executeWrite(jobLog: JobLog, df: DataFrame, step: WorkflowStep, variables: Variables): Unit = {
    val stepLog = jobLog.getStepLog(step.step)
    val incrementalType = jobLog.logDrivenType
    ETLLogger.info(s"incremental type is ${incrementalType}")
    if (incrementalType == IncrementalType.DIFF && df.count() > incrementalDiffModeDataLimit.toLong) {
      throw IncrementalDiffModeTooMuchDataException(
        s"Incremental diff mode data limit is $incrementalDiffModeDataLimit, but current data count is ${df.count()}"
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
      val count = if (step.target.dataSourceType == DataSourceType.VARIABLES) 0 else df.count().toInt
      stepLog.targetCount = count
      ETLLogger.info("[Physical Plan]:")
      df.explain()
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
    var df = IO.read(spark, step, variables, jobLog)
    if (df != null) {
      if (step.getRepartition != null) {
        df = step.getRepartition match {
          case Pattern.REPARTITION_NUM_PATTERN() =>
            df.repartition(step.getRepartition.toInt)
          case Pattern.REPARTITION_COLUMNS_PATTERN(_) =>
            df.repartition(ConvertUtils.strsToColumns(step.getRepartition.split(",")): _*)
          case Pattern.REPARTITION_NUM_COLUMNS_PATTERN(_) =>
            val repartitionNumAndColumns = step.getRepartition.split(",")
            df.repartition(
              repartitionNumAndColumns.head.toInt,
              ConvertUtils.strsToColumns(
                repartitionNumAndColumns.slice(1, repartitionNumAndColumns.length)
              ): _*
            )
          case _ =>
            val errorMessage = s"Unknown input: ${step.getRepartition}."
            stepLog.error(errorMessage)
            throw new RuntimeException(errorMessage)
        }
      }
      if (step.coalesce != null) {
        df.coalesce(step.coalesce.toInt)
      }
      if (step.getPersist != null) {
        df.persist(StorageLevel.fromString(step.getPersist))
      }
      if (step.getCheckPoint != null && step.getCheckPoint.toBoolean) {
        df.localCheckpoint()
      }
      stepLog.sourceCount = if (step.target.dataSourceType == DataSourceType.VARIABLES) 0 else df.count().toInt
    }
    df
  }
  // scalastyle:on


  /**
   * 释放资源
   */
  override def close(): Unit = {
    try {
      ETLSparkSession.release(spark)
    } catch {
      case NonFatal(e) =>
        ETLLogger.error("Stop Spark session failed", e)
    }
  }

  override def applyConf(conf: Map[String, String]): Unit = {
    conf.foreach {
      case (key, value) =>
        ETLLogger.warn(s"Setting spark conf $key=$value")
        spark.conf.set(key, value)
    }
  }

  override def applicationId(): String = sparkSession.sparkContext.applicationId
}
