package com.github.sharpdata.sharpetl.core.notification.sender.email

import com.github.sharpdata.sharpetl.core.notification.sender.{Notification, NotificationSender, NotificationType}
import com.github.sharpdata.sharpetl.core.util.ETLLogger

import java.util.{Date, Properties}
import javax.activation.DataHandler
import javax.mail.internet.{InternetAddress, MimeBodyPart, MimeMessage, MimeMultipart}
import javax.mail.util.ByteArrayDataSource
import javax.mail.{Address, Message, Session, Transport}

// $COVERAGE-OFF$
class EmailSender(val emailSenderConfiguration: EmailSenderConfiguration) extends NotificationSender {

  override def send(notification: Notification): Unit = {

    val email = notification.asInstanceOf[Email]
    val props = emailSenderConfiguration.toSessionProperties()

    val session = Session.getInstance(props)
    try {
      val message = new MimeMessage(session)
      message.addHeader("Content-Transfer-Encoding", "8bit")

      message.setFrom(new InternetAddress(email.sender.address, email.sender.personalName))
      message.setReplyTo(InternetAddress.parse(email.sender.address, false).map(it => it.asInstanceOf[Address]))

      val multipart = new MimeMultipart()

      val emailBody = new MimeBodyPart()
      emailBody.setText(email.body)
      multipart.addBodyPart(emailBody)

      if (email.attachment.isDefined) {
        val attachment = new MimeBodyPart()
        val dataSource = new ByteArrayDataSource(
          email.attachment.get.content.getBytes(),
          email.attachment.get.mimeType)
        attachment.setDataHandler(new DataHandler(dataSource))
        attachment.setFileName(email.attachment.get.fileName)
        multipart.addBodyPart(attachment)
      }

      message.setContent(multipart)
      message.setSubject(email.subject)
      message.setSentDate(new Date())
      message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(email.receiver, false).map(it => it.asInstanceOf[Address]))

      Transport.send(message)
    } catch {
      case e: Throwable =>
        ETLLogger.error("Failed to send email", e)
        throw e
    }
  }
}

class Email(val sender: Sender,
            val receiver: String,
            val subject: String,
            val body: String,
            val attachment: Option[EmailAttachment]) extends Notification(body, NotificationType.EMAIL)


final case class Sender(address: String, personalName: String)


class EmailAttachment(val content: String,
                      val mimeType: String,
                      val fileName: String)

class EmailSenderConfiguration(
                              val host: String,
                              val port: Int
                              ) {
  def toSessionProperties(): Properties = {
    val props = new Properties()
    props.put("mail.smtp.host", host)
    props.put("mail.smtp.port", port.toString)
    props
  }
}

object EmailSenderConfiguration {

  private val defaultPort = 25
  private val defaultHost = "localhost"


  def init(props: Map[String, String]): EmailSenderConfiguration = {
    new EmailSenderConfiguration(
      props.getOrElse("host", defaultHost),
      props.get("port").map(s => s.toInt).getOrElse(defaultPort)
    )
  }


}
// $COVERAGE-ON$
