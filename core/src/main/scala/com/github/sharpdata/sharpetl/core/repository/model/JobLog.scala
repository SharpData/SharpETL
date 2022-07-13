package com.github.sharpdata.sharpetl.core.repository.model

import com.github.sharpdata.sharpetl.core.repository.model.JobStatus.{FAILURE, SUCCESS}

import java.time.LocalDateTime
import scala.beans.BeanProperty
import scala.collection.mutable

object JobStatus {
  val SUCCESS = "SUCCESS"
  val FAILURE = "FAILURE"
  val RUNNING = "RUNNING"
}

object Constants {
  val NULL_DATETIME: LocalDateTime = null // scalastyle:ignore
  val NULL_INTEGER: Int = 0
}


class JobLog(
              @BeanProperty
              var jobId: Long,
              @BeanProperty
              var jobName: String,
              @BeanProperty
              var jobPeriod: Int,
              @BeanProperty
              var jobScheduleId: String,
              @BeanProperty
              var dataRangeStart: String,
              @BeanProperty
              var dataRangeEnd: String,
              @BeanProperty
              var jobStartTime: LocalDateTime,
              @BeanProperty
              var jobEndTime: LocalDateTime,
              @BeanProperty
              var status: String,
              @BeanProperty
              var createTime: LocalDateTime,
              @BeanProperty
              var lastUpdateTime: LocalDateTime,
              @BeanProperty
              var incrementalType: String,
              @BeanProperty
              var currentFile: String,
              @BeanProperty
              var applicationId: String,
              @BeanProperty
              var projectName: String
            ) extends Serializable {
  private val stepLogs: mutable.Map[String, StepLog] = mutable.Map()

  def failed(): Unit = {
    status = FAILURE
  }

  def success(): Unit = {
    status = SUCCESS
  }

  def getStepLog(stepId: String): StepLog = {
    stepLogs.getOrElseUpdate(stepId, createStepLog(stepId))
  }

  def setStepLogs(stepLogs: Array[StepLog]): Unit = {
    for (elem <- stepLogs) {
      this.stepLogs(elem.stepId) = elem
    }
  }

  def getStepLogs(): Array[StepLog] = {
    stepLogs.values.toArray.sortBy(_.stepId)
  }

  def createStepLog(stepId: String): StepLog = {
    val stepLog = new StepLog(
      jobId = this.jobId,
      stepId = stepId,
      status = JobStatus.RUNNING,
      startTime = LocalDateTime.now(),
      endTime = Constants.NULL_DATETIME,
      duration = Constants.NULL_INTEGER,
      output = "",
      error = "",
      sourceCount = Constants.NULL_INTEGER,
      targetCount = Constants.NULL_INTEGER,
      successCount = Constants.NULL_INTEGER,
      failureCount = Constants.NULL_INTEGER,
      sourceType = "",
      targetType = ""
    )
    stepLogs(stepId) = stepLog
    stepLog
  }

}
