package com.github.sharpdata.sharpetl.spark.transformation

import com.github.sharpdata.sharpetl.core.notification.NotificationUtil
import com.github.sharpdata.sharpetl.core.notification.sender.NotificationFactory
import com.github.sharpdata.sharpetl.core.notification.sender.email.{Email, EmailAttachment, Sender}
import com.github.sharpdata.sharpetl.core.repository.JobLogAccessor.jobLogAccessor
import com.github.sharpdata.sharpetl.core.repository.StepLogAccessor.stepLogAccessor
import com.github.sharpdata.sharpetl.core.repository.model.JobLog
import com.github.sharpdata.sharpetl.core.util.Constants.Environment
import com.github.sharpdata.sharpetl.core.util.DateUtil.{L_YYYY_MM_DD_HH_MM_SS, YYYYMMDDHHMMSS}
import com.github.sharpdata.sharpetl.core.util.JobLogUtil.JogLogExternal
import JobLogDFConverter.JobLogList
import com.github.sharpdata.sharpetl.spark.utils.ETLSparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

import java.time.LocalDateTime
import scala.collection.immutable.ListMap
import scala.util.{Failure, Success, Try}

object DailyJobsSummaryReportTransformer extends Transformer {
  private lazy val notificationService = new NotificationUtil(jobLogAccessor)

  override def transform(args: Map[String, String]): DataFrame = {
    sendAllJobsSummaryReport(
      LocalDateTime.parse(args("dataRangeStart"), L_YYYY_MM_DD_HH_MM_SS),
      LocalDateTime.parse(args("dataRangeEnd"), L_YYYY_MM_DD_HH_MM_SS),
      args.get("datasource").map(_.split(",")).getOrElse(Array.empty))
    ETLSparkSession.sparkSession.emptyDataFrame
  }

  def sendAllJobsSummaryReport(startTime: LocalDateTime, endTime: LocalDateTime, datasource: Array[String]): Unit = {
    val jobLogs = jobLogAccessor.executionsBetween(startTime, endTime)

    if (jobLogs.nonEmpty) {
      val stepLogs = stepLogAccessor.stepLogsBetween(startTime, endTime).groupBy(_.jobId)

      val allJobLogs: Array[JobLog] = ListMap(
        jobLogs
          .groupBy(jobLog => (jobLog.projectName, jobLog.jobName))
          .toSeq.sortBy(_._1): _*
      )
        .mapValues(_.sortBy(_.dataRangeStart))
        .values.flatten
        .map(jobLog => {
          jobLog.setStepLogs(stepLogs.getOrElse(jobLog.jobId, Array.empty))
          jobLog
        }
        ).toArray

      val dataframe = allJobLogs.toDF(datasource)
      val headers = dataframe.schema.map(_.name).mkString(",")
      val content = dataframe.collect().map(_.mkString(",")).mkString("\n")
      val csvText =
        s"""$headers
           |$content""".stripMargin

      val startTimeText = startTime.format(YYYYMMDDHHMMSS)
      val endTimeText = endTime.format(YYYYMMDDHHMMSS)

      NotificationFactory.sendNotification(
        new Email(
          Sender(notificationService.emailSender, notificationService.emailSenderPersonalName),
          notificationService.summaryJobReceivers,
          s"[${Environment.current.toUpperCase}] Daily ETL Job summary report($startTimeText to $endTimeText)",
          s"Attachment is report of all jobs between $startTimeText to $endTimeText",
          Option.apply(new EmailAttachment(csvText, "text/csv", s"attachment-$startTimeText-$endTimeText.csv")))
      )
    }
  }
}

object JobLogDFConverter {
  implicit class JobLogList(jobLogs: Array[JobLog]) {
    def toDF(datasource: Array[String]): DataFrame = {
      if (jobLogs.isEmpty) {
        ETLSparkSession.sparkSession.emptyDataFrame
      } else {
        val rows = jobLogs.map(jobLog => {
          val value = Array(
            jobLog.projectName,
            jobLog.jobName,
            jobLog.jobId.toString,
            Try(LocalDateTime.parse(jobLog.dataRangeStart, YYYYMMDDHHMMSS)) match {
              case Failure(_) => jobLog.dataRangeStart
              case Success(value) => value.format(L_YYYY_MM_DD_HH_MM_SS)
            },
            Try(LocalDateTime.parse(jobLog.dataRangeEnd, YYYYMMDDHHMMSS)) match {
              case Failure(_) => jobLog.dataRangeStart
              case Success(value) => value.format(L_YYYY_MM_DD_HH_MM_SS)
            },
            jobLog.jobStartTime.format(L_YYYY_MM_DD_HH_MM_SS),
            jobLog.status,
            jobLog.duration().toString,
            jobLog.dataFlow()
          )

          val errorMessage = jobLog.errorMessage()
          val extendValue = datasource.map(datasource =>
            jobLog.getStepLogs().find(step => step.targetType == datasource).map(_.successCount.toString).getOrElse(""))
          Row.fromSeq(value ++ extendValue ++ Array(jobLog.failStep(), s""" "$errorMessage" """.trim))
        }
        )

        val fields = Array(
          StructField("projectName", StringType, true),
          StructField("jobName", StringType, true),
          StructField("jobId", StringType, true),
          StructField("dataRangeStart", StringType, true),
          StructField("dataRangeEnd", StringType, true),
          StructField("jobStartTime", StringType, true),
          StructField("jobStatus", StringType, true),
          StructField("duration(seconds)", StringType, true),
          StructField("dataFlow", StringType, true)
        ) ++ datasource.map(datasource => StructField(s"to-$datasource", StringType, true)) ++
          Array(
            StructField("failStep", StringType, true),
            StructField("errorMessage", StringType, true))

        val value: RDD[Row] = ETLSparkSession.sparkSession.sparkContext.parallelize(rows)
        ETLSparkSession.sparkSession.createDataFrame(
          rowRDD = value,
          schema = StructType(fields))

      }
    }
  }
}


