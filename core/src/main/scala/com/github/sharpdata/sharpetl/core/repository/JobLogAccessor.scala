package com.github.sharpdata.sharpetl.core.repository

import com.github.sharpdata.sharpetl.core.repository.model.JobLog
import com.github.sharpdata.sharpetl.core.repository.model.JobLog
import com.github.sharpdata.sharpetl.core.util.{Constants, JDBCUtil}

import java.time.LocalDateTime

abstract class JobLogAccessor() {
  def lastSuccessExecuted(jobName: String): JobLog

  def lastExecuted(jobName: String): JobLog

  def executionsBetween(startTime: LocalDateTime, endTime: LocalDateTime): Array[JobLog]

  def executionsLastYear(jobName: String): Array[JobLog]

  def isAnotherJobRunning(jobScheduleId: String): JobLog

  def create(jobLog: JobLog): Unit = {
    jobLog.jobStartTime = LocalDateTime.now()
    jobLog.createTime = LocalDateTime.now()
    jobLog.lastUpdateTime = LocalDateTime.now()
  }

  def update(jobLog: JobLog): Unit = {
    jobLog.lastUpdateTime = LocalDateTime.now()
  }

  def updateStatus(jobLog: JobLog): Unit = {
    jobLog.lastUpdateTime = LocalDateTime.now()
  }

  def getLatestSuccessJobLogByNames(jobNames: Array[String]): Array[JobLog]

  def getPreviousJobLog(jobLog: JobLog): JobLog

  def getUnprocessedUpstreamJobLog(upstreamJobName: String, upstreamLogId: BigInt): Array[JobLog]
}

object JobLogAccessor {
  lazy val jobLogAccessor: JobLogAccessor = JobLogAccessor.getInstance(JDBCUtil.dbType)

  private def getInstance(databaseType: String): JobLogAccessor = {
    databaseType match {
      case Constants.ETLDatabaseType.MSSQL => new com.github.sharpdata.sharpetl.core.repository.mssql.JobLogAccessor()
      case Constants.ETLDatabaseType.H2 => new com.github.sharpdata.sharpetl.core.repository.mysql.JobLogAccessor()
      case Constants.ETLDatabaseType.MYSQL => new com.github.sharpdata.sharpetl.core.repository.mysql.JobLogAccessor()
    }
  }
}
