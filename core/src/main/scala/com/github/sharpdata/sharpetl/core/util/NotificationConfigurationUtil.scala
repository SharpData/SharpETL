package com.github.sharpdata.sharpetl.core.util

import cats.syntax.either._
import com.github.sharpdata.sharpetl.core.exception.Exception.BadYamlFileException
import io.circe._
import io.circe.generic.extras.Configuration
import io.circe.yaml.parser
import io.circe.generic.extras.auto._



@deprecated
object NotificationConfigurationUtil {

  private lazy val notificationConfigFilePath: String = ETLConfig.getProperty("notification.config.path")
  private lazy val notificationConfig: Option[NotificationConfiguration] = {
    if (notificationConfigFilePath == null) {
      ETLLogger.warn("No notification config file is set!")
      None
    } else {
      Some(readNotificationConfig())
    }
  }

  def getNotificationConfigs(): List[GroupedJobNotifyConfig] =
    notificationConfig.map(_.notifications).getOrElse(List.empty)

  def getProjectNames(): List[ProjectJobNames] = notificationConfig.map(_.projectJobNames).getOrElse(List.empty)

  private def readNotificationConfig(): NotificationConfiguration = {
    val yamlString = if (notificationConfigFilePath.startsWith("hdfs")) {
      HDFSUtil.readLines(notificationConfigFilePath).mkString("\n")
    } else {
      IOUtil.readLinesFromText(notificationConfigFilePath).mkString("\n")
    }

    implicit val circeConfig: Configuration = Configuration.default.withDefaults

    parser.parse(yamlString)
      .leftMap(err => err: Error)
      .flatMap(_.as[NotificationConfiguration])
      .valueOr(it =>
        throw BadYamlFileException(
        s"can not convert file to notification config, error: ${it.getMessage}")
      )
  }
}


final case class NotificationConfiguration(projectJobNames: List[ProjectJobNames],
                                           notifications: List[GroupedJobNotifyConfig])

case class GroupedJobNotifyConfig(jobNames: String = "",
                                  projectName: String = "",
                                  triggerCondition: String,
                                  notifies: List[Notify])


final case class Notify(notifyType: String, recipients: List[String])

final case class ProjectJobNames(projectName: String,
                                 jobNames: List[String])
