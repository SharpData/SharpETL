package com.github.sharpdata.sharpetl.core.repository.model

import com.github.sharpdata.sharpetl.core.exception.Exception.throwableAsString
import com.github.sharpdata.sharpetl.core.repository.model.StepLog.ERROR_DEFAULT_TRUNCATION
import org.apache.log4j.Logger

import java.lang.reflect.InvocationTargetException
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import scala.beans.BeanProperty

object StepStatus {
  val SUCCESS = "SUCCESS"
  val FAILURE = "FAILURE"
  val RUNNING = "RUNNING"
}

class StepLog(
               @BeanProperty
               var jobId: String,
               @BeanProperty
               var stepId: String,
               @BeanProperty
               var status: String,
               @BeanProperty
               var startTime: LocalDateTime,
               @BeanProperty
               var endTime: LocalDateTime,
               @BeanProperty
               var duration: Int,
               @BeanProperty
               var output: String,
               @BeanProperty
               var error: String,
               @BeanProperty
               var sourceCount: Integer,
               @BeanProperty
               var targetCount: Integer,
               @BeanProperty
               var successCount: Integer,
               @BeanProperty
               var failureCount: Integer,
               @BeanProperty
               var sourceType: String,
               @BeanProperty
               var targetType: String) {
  val logger: Logger = Logger.getLogger("ETLLogger")

  def failed(errorMsg: String): Unit = {
    error(errorMsg)
    status = StepStatus.FAILURE
    endTime = LocalDateTime.now()
    duration = startTime.until(endTime, ChronoUnit.SECONDS).toInt
  }

  def failed(error: Throwable): Unit = {
    val prefix = error.getClass.getName + ": "
    val message =
      if (error.getCause != null && error.getCause.isInstanceOf[InvocationTargetException]) {
        // get transformer error
        val e = error.getCause.asInstanceOf[InvocationTargetException]
        e.getCause.getMessage
      } else if (error.getMessage != null && error.getMessage.nonEmpty) {
        error.getMessage
      } else if (error.getCause != null) {
        error.getCause.getMessage
      } else {
        throwableAsString(error)
      }
    failed(prefix + message)
  }

  def success(): Unit = {
    status = StepStatus.SUCCESS
    endTime = LocalDateTime.now()
    duration = startTime.until(endTime, ChronoUnit.SECONDS).toInt
  }

  def error(str: String): Unit = {
    if (str != null && str.nonEmpty) {
      error = error + "\n[error] " + str.take(ERROR_DEFAULT_TRUNCATION)
      logger.error(str)
    }
  }

  def info(str: String): Unit = {
    output = output + "\n[info] " + str.take(ERROR_DEFAULT_TRUNCATION)
    logger.info(str)
  }

}

object StepLog {
  val ERROR_DEFAULT_TRUNCATION = 5000
}
