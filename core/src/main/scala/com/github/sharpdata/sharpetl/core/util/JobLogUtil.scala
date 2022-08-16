package com.github.sharpdata.sharpetl.core.util

import com.github.sharpdata.sharpetl.core.repository.model.{JobLog, JobStatus}
import com.github.sharpdata.sharpetl.core.util.Constants.PeriodType.{DAY, HOUR}
import com.github.sharpdata.sharpetl.core.util.Constants.{DataSourceType, IncrementalType}
import com.github.sharpdata.sharpetl.core.util.DateUtil.{L_YYYY_MM_DD_HH_MM_SS, YYYYMMDDHHMMSS}

import java.time.LocalDateTime

object JobLogUtil {

  implicit class JogLogExternal(jobLog: JobLog) {

    def dataFlow(): String = {
      if (jobLog.getStepLogs().isEmpty) {
        ""
      } else {
        val stepLogs = jobLog.getStepLogs()
        val filterStepLogs = stepLogs.filter(stepLog =>
          stepLog.targetType.nonEmpty
            && stepLog.targetType != DataSourceType.DO_NOTHING
            && stepLog.targetType != DataSourceType.VARIABLES
            && stepLog.targetType != DataSourceType.TEMP)

        if (filterStepLogs.length == 1) {
          val head = filterStepLogs.head
          s"${head.sourceType}(${head.sourceCount}) -> ${head.targetType}(${head.successCount})"
        } else {
          filterStepLogs
            .map(stepLog => s"${stepLog.targetType}(${stepLog.successCount})")
            .mkString(" -> ")
        }
      }
    }

    def failStep(): String = {
      if (jobLog.status != JobStatus.FAILURE || jobLog.getStepLogs().isEmpty) {
        ""
      } else {
        jobLog.getStepLogs().reverse.head.stepId
      }
    }

    def errorMessage(): String = {
      if (jobLog.status != JobStatus.FAILURE || jobLog.getStepLogs().isEmpty) {
        ""
      } else {
        jobLog.getStepLogs().reverse.head.error.replace("\n", " ")
      }
    }

    def duration(): Int = {
      if (jobLog.getStepLogs().isEmpty) {
        0
      } else {
        jobLog.getStepLogs().map(_.duration).sum
      }
    }
  }

  implicit class JobLogFormatter(jobLog: JobLog) {
    def formatDataRangeStart(): String =
      jobLog.logDrivenType match {
        case IncrementalType.AUTO_INC_ID => jobLog.dataRangeStart
        case IncrementalType.KAFKA_OFFSET => jobLog.dataRangeStart
        case IncrementalType.UPSTREAM => jobLog.dataRangeStart
        case _ => LocalDateTime.parse(jobLog.dataRangeStart, YYYYMMDDHHMMSS).format(L_YYYY_MM_DD_HH_MM_SS)
      }

    def formatDataRangeEnd(): String =
      jobLog.logDrivenType match {
        case IncrementalType.AUTO_INC_ID => jobLog.dataRangeEnd
        case IncrementalType.KAFKA_OFFSET => jobLog.dataRangeEnd
        case IncrementalType.UPSTREAM => jobLog.dataRangeEnd
        case _ => LocalDateTime.parse(jobLog.dataRangeEnd, YYYYMMDDHHMMSS).format(L_YYYY_MM_DD_HH_MM_SS)
      }

    def jobTimeBase(jobLog: JobLog): Boolean = {
      !jobLog.logDrivenType.equals(IncrementalType.AUTO_INC_ID) &&
        !jobLog.logDrivenType.equals(IncrementalType.KAFKA_OFFSET) &&
        !jobLog.logDrivenType.equals(IncrementalType.UPSTREAM)
    }

    def defaultTimePartition(): scala.collection.mutable.Map[String, String] = {
      val timePartitionArg = scala.collection.mutable.Map[String, String]()

      if (jobLog.logDrivenType == null || jobTimeBase(jobLog)) {
        val startDate = LocalDateTime.parse(jobLog.dataRangeStart, YYYYMMDDHHMMSS)
        timePartitionArg.put("${YEAR}", startDate.getYear.toString)
        val month = startDate.getMonthValue
        if (month < 10) {
          timePartitionArg.put("${MONTH}", s"0$month")
        } else {
          timePartitionArg.put("${MONTH}", month.toString)
        }
        if (jobLog.period % DAY == 0) {
          val day = startDate.getDayOfMonth
          if (day < 10) {
            timePartitionArg.put("${DAY}", s"0$day")
          } else {
            timePartitionArg.put("${DAY}", day.toString)
          }
        } else if (jobLog.period % HOUR == 0) {
          val hour = startDate.getHour
          if (hour < 10) {
            timePartitionArg.put("${HOUR}", s"0$hour")
          } else {
            timePartitionArg.put("${HOUR}", hour.toString)
          }
        } else {
          val minute = startDate.getMinute
          if (minute < 10) {
            timePartitionArg.put("${MINUTE}", s"0$minute")
          } else {
            timePartitionArg.put("${MINUTE}", minute.toString)
          }
        }
      }
      timePartitionArg
    }
  }
}
