package com.github.sharpdata.sharpetl.core.notification

import com.github.sharpdata.sharpetl.core.notification.sender.{NotificationFactory, NotificationType}
import com.github.sharpdata.sharpetl.core.repository.JobLogAccessor
import com.github.sharpdata.sharpetl.core.repository.model.{JobLog, JobStatus}
import com.github.sharpdata.sharpetl.core.util.Constants.Environment
import com.github.sharpdata.sharpetl.core.util.{ETLConfig, ETLLogger}
import com.github.sharpdata.sharpetl.core.notification.sender.email.{Email, Sender}
import com.github.sharpdata.sharpetl.core.notification.sender.{NotificationFactory, NotificationType}
import com.github.sharpdata.sharpetl.core.repository.JobLogAccessor
import com.github.sharpdata.sharpetl.core.repository.model.{JobLog, JobStatus}
import com.github.sharpdata.sharpetl.core.util.Constants.Environment
import com.github.sharpdata.sharpetl.core.util.DateUtil.{L_YYYY_MM_DD_HH_MM_SS, YYYYMMDDHHMMSS}
import com.github.sharpdata.sharpetl.core.util.JobLogUtil.JogLogExternal
import com.github.sharpdata.sharpetl.core.util._

import java.time.LocalDateTime

class NotificationService(val jobLogAccessor: JobLogAccessor) {

  lazy val notificationConfigFilePath: String = ETLConfig.getProperty("notification.config.path")
  lazy val emailSender: String = ETLConfig.getProperty("notification.email.sender")
  lazy val emailSenderPersonalName: String = ETLConfig.getProperty("notification.email.senderPersonalName")
  lazy val summaryJobReceivers: String = ETLConfig.getProperty("notification.email.summaryReceivers")

  def sendNotification(jobResult: Seq[Seq[Try[JobLog]]]): Unit = {
    val jobNames = jobResult.flatten.map(_.get.jobName).toSet

    val notifyTypeWithRecipientToNotifies =
      getJobNotifyConfigs()
        .filter(notify => jobNames.contains(notify.jobName))
        .groupBy(notify => (notify.notifyType, notify.recipient))

    val jobNameToJobLog: Map[String, Seq[JobLog]] = jobResult.flatten.map(_.get).groupBy(_.jobName)

    notifyTypeWithRecipientToNotifies
      .foreach { case ((notifyType, recipient), notifyConfigs) =>
        val jobNotifications = gropingNotification(notifyConfigs, jobNameToJobLog)
        if (jobNotifications.nonEmpty) {
          val messages = jobNotifications
            .map(_.toString())
            .mkString("\n\n")
          ETLLogger.info(s"Notification message:\n $messages")
          val notification = notifyType match {
            case NotificationType.EMAIL =>
              new Email(Sender(emailSender, emailSenderPersonalName),
                recipient, s"[${Environment.current.toUpperCase}] ETL job summary report", messages, Option.empty)
            case _ => ???
          }
          NotificationFactory.sendNotification(notification)
        }
      }
  }

  def shouldSendEmail(notifyConfig: JobNotifyConfig, jobLog: JobLog): Boolean = {
    if (notifyConfig.triggerCondition != NotifyTriggerCondition.FAILURE) {
      notifyConfig.accept(jobLog)
    } else if (!notifyConfig.accept(jobLog)) {
      false
    } else {
      val previousJobLog = jobLogAccessor.getPreviousJobLog(jobLog)
      previousJobLog == null || previousJobLog.status != JobStatus.FAILURE
    }
  }

  private def gropingNotification(jobNotifyConfigs: List[JobNotifyConfig], jobNamesToJobLog: Map[String, Seq[JobLog]]) = {

    jobNotifyConfigs.flatMap(notifyConfig => {
      val jobName = notifyConfig.jobName
      val jobLogs = jobNamesToJobLog.getOrElse(jobName, Seq.empty)
      jobLogs.filter(shouldSendEmail(notifyConfig, _))
    })
      .groupBy(jobLog => (jobLog.jobName, jobLog.jobId))
      .map(_._2.head)
      .map(jobLog => {
        buildJobMessage(jobLog)
      }
      ).toList
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

  private def getJobNotifyConfigs(): List[JobNotifyConfig] = {
    val notifyConfigs = NotificationConfigurationUtil.getNotificationConfigs()
    if (notifyConfigs == null) {
      ETLLogger.warn("No notification config file is set!")
      List.empty
    } else {
      notifyConfigs
        .flatMap(notification => {
          val jobNames = if (notification.jobNames.isEmpty) {
            ProjectJobNameUtil.getJobNames(notification.projectName)
          } else {
            notification.jobNames.split(",")
          }
          jobNames.flatMap(
            name => notification.notifies.flatMap(notify => notify.recipients
              .map(recipient => JobNotifyConfig(notify.notifyType, name.trim, notification.triggerCondition, recipient))
            ))
        })
    }
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


final case class JobNotifyConfig(notifyType: String,
                                 jobName: String,
                                 triggerCondition: String,
                                 recipient: String) {

  def accept(jobLog: JobLog): Boolean = {
    jobLog.jobName == jobName && (
      triggerCondition == NotifyTriggerCondition.ALWAYS || triggerCondition == jobLog.status
      )
  }
}
