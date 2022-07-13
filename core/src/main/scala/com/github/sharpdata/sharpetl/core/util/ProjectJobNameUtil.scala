package com.github.sharpdata.sharpetl.core.util

@deprecated
object ProjectJobNameUtil {

  val getProjectName: Memo1[String, String] = Memo1 { (jobName: String) => doGetProjectName(jobName) }
  val getJobNames: Memo1[String, Array[String]] = Memo1 { (projectName: String) => doGetJobNames(projectName) }

  private def doGetProjectName(jobName: String): String = {
    NotificationConfigurationUtil.getProjectNames()
      .find(_.jobNames.contains(jobName))
      .map(_.projectName).getOrElse("")
  }

  private def doGetJobNames(projectName: String): Array[String] = {
    NotificationConfigurationUtil.getProjectNames()
      .find(_.projectName == projectName)
      .map(_.jobNames.toArray).getOrElse(Array[String]())
  }
}
