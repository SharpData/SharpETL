package com.github.sharpdata.sharpetl.core.notification.sender

import com.github.sharpdata.sharpetl.core.notification.sender.email.EmailSenderConfiguration
import com.github.sharpdata.sharpetl.core.util.ETLConfig
import com.github.sharpdata.sharpetl.core.notification.sender.email.{EmailSender, EmailSenderConfiguration}
import com.github.sharpdata.sharpetl.core.util.ETLConfig

object NotificationFactory {

  val senders: Map[String, NotificationSender] = initAllSender()

  def sendNotification(notification: Notification): Unit = {
    notification.notificationType match {
      case NotificationType.EMAIL => senders(NotificationType.EMAIL).send(notification)
      case _ => ???
    }
  }

  def initAllSender(): Map[String, NotificationSender] = {
    if (ETLConfig.getProperties("notification.smtp").nonEmpty) {
      val smtpProps = ETLConfig.getProperties("notification.smtp")
      Map(NotificationType.EMAIL ->
        new EmailSender(EmailSenderConfiguration.init(smtpProps)))
    } else {
      Map.empty
    }
  }
}


trait NotificationSender {
  def send(notification: Notification): Unit
}

abstract class Notification(val message: String, val notificationType: String)

object NotificationType {
  val EMAIL = "email"
}
