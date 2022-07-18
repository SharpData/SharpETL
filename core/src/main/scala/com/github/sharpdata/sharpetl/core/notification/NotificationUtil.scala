package com.github.sharpdata.sharpetl.core.notification

import com.github.sharpdata.sharpetl.core.api.WFInterpretingResult
import com.github.sharpdata.sharpetl.core.notification.sender.{NotificationFactory, NotificationType}
import com.github.sharpdata.sharpetl.core.repository.JobLogAccessor
import com.github.sharpdata.sharpetl.core.repository.model.{JobLog, JobStatus}
import com.github.sharpdata.sharpetl.core.util.Constants.Environment
import com.github.sharpdata.sharpetl.core.util.{ETLConfig, ETLLogger}
import com.github.sharpdata.sharpetl.core.notification.sender.email.{Email, Sender}
import com.github.sharpdata.sharpetl.core.util.DateUtil.{L_YYYY_MM_DD_HH_MM_SS, YYYYMMDDHHMMSS}
import com.github.sharpdata.sharpetl.core.util.JobLogUtil.JogLogExternal
import com.google.common.base.Strings.isNullOrEmpty

import java.time.LocalDateTime

class NotificationUtil(val jobLogAccessor: JobLogAccessor) {

  lazy val emailSender: String = ETLConfig.getProperty("notification.email.sender")
  lazy val emailSenderPersonalName: String = ETLConfig.getProperty("notification.email.senderPersonalName")
  lazy val summaryJobReceivers: String = ETLConfig.getProperty("notification.email.summaryReceivers")

  def notify(jobResults: Seq[WFInterpretingResult]): Unit = {
    val configToLogs: Seq[(NotifyConfig, Seq[JobLog])] =
      jobResults
        .filterNot(it => it.workflow.notifys == null || it.workflow.notifys.isEmpty)
        .flatMap(it =>
          it.workflow.notifys
            .flatMap(_.toConfigs())
            .map(n => (n, it.jobLogs.map(_.get)))
            .map { case (config, logs) =>
              (config,
                logs.filter(shouldNotify(config, _)))
            }
        )

    configToLogs
      .map(it => (it._1, it._2.map(buildJobMessage)))
      .groupBy(_._1)
      .foreach(it => {
        val messages = it._2
          .flatMap(_._2)
          .map(_.toString)
          .mkString("\n\n")
        ETLLogger.info(s"Notification message:\n $messages")

        if (!isNullOrEmpty(messages)) {
          val notification = it._1.notifyType match {
            case NotificationType.EMAIL =>
              new Email(Sender(emailSender, emailSenderPersonalName),
                it._1.recipient, s"[${Environment.current.toUpperCase}] ETL job summary report", messages)
            case _ => ???
          }
          NotificationFactory.sendNotification(notification)
        }
      })
  }

  def shouldNotify(notifyConfig: NotifyConfig, jobLog: JobLog): Boolean = {
    if (notifyConfig.triggerCondition != NotifyTriggerCondition.FAILURE) {
      notifyConfig.accept(jobLog)
    } else if (!notifyConfig.accept(jobLog)) {
      false
    } else {
      val previousJobLog = jobLogAccessor.getPreviousJobLog(jobLog)
      previousJobLog == null || previousJobLog.status != JobStatus.FAILURE
    }
  }

  def buildJobMessage(jobLog: JobLog): JobMessage = {

    JobMessage(
      jobId = jobLog.jobId,
      jobName = jobLog.jobName,
      jobRangeStart = scala.util.Try(LocalDateTime.parse(jobLog.dataRangeStart, YYYYMMDDHHMMSS)) match {
        case scala.util.Failure(_) => jobLog.dataRangeStart
        case scala.util.Success(value) => value.format(L_YYYY_MM_DD_HH_MM_SS)
      },
      jobRangeEnd = scala.util.Try(LocalDateTime.parse(jobLog.dataRangeEnd, YYYYMMDDHHMMSS)) match {
        case scala.util.Failure(_) => jobLog.dataRangeEnd
        case scala.util.Success(value) => value.format(L_YYYY_MM_DD_HH_MM_SS)
      },
      jobStatus = jobLog.status,
      errorMessage = jobLog.errorMessage(),
      failStep = jobLog.failStep(),
      applicationId = jobLog.applicationId,
      projectName = jobLog.projectName,
      dataFlow = jobLog.dataFlow(),
      duration = jobLog.duration().toString,
      jobStartTime = jobLog.jobStartTime.format(L_YYYY_MM_DD_HH_MM_SS)
    )
  }
}

final case class JobMessage(jobId: Long,
                            jobName: String,
                            jobRangeStart: String,
                            jobRangeEnd: String,
                            jobStatus: String,
                            errorMessage: String,
                            failStep: String,
                            applicationId: String,
                            projectName: String,
                            dataFlow: String,
                            duration: String,
                            jobStartTime: String) {

  override def toString: String = {
    val text =
      s"""
         |projectName: $projectName,
         |jobId: $jobId,
         |jobName: $jobName,
         |jobRangeStart: $jobRangeStart,
         |jobRangeEnd: $jobRangeEnd,
         |jobStatus: $jobStatus,
         |dataFlow: $dataFlow,
         |applicationId: $applicationId
         |""".stripMargin
    if (jobStatus == JobStatus.FAILURE) {
      text +
        s"""
           |failStep: $failStep
           |errorMessage: $errorMessage""".stripMargin
    } else {
      text
    }
  }
}

object NotifyTriggerCondition {
  val FAILURE: String = "FAILURE"
  val ALWAYS: String = "ALWAYS"
  val SUCCESS: String = "SUCCESS"
}

final case class NotifyConfig(notifyType: String,
                              recipient: String,
                              triggerCondition: String) {

  def accept(jobLog: JobLog): Boolean = {
    triggerCondition == NotifyTriggerCondition.ALWAYS || triggerCondition == jobLog.status
  }
}
