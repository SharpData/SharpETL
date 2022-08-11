package com.github.sharpdata.sharpetl.core.syntax

import com.github.sharpdata.sharpetl.core.annotation.Annotations.Evolving
import com.github.sharpdata.sharpetl.core.util.Constants.Separator.ENTER
import com.github.sharpdata.sharpetl.core.util.StringUtil

@Evolving(since = "1.0.0")
final case class Workflow(
                           name: String,//workflowName
                           period: String,//, "",
                           loadType: String,//dwdTableConfig.updateType,
                           logDrivenType: String,//"upstream", //TODO: update later
                           upstream: String,//s"ods__${dwdModding.dwdTableConfig.sourceTable}",
                           dependsOn: String,//null
                           comment: String,//null
                           timeout: Int,//0
                           defaultStart: String,//null
                           stopScheduleWhenFail: Boolean,//fasle
                           notifies: Seq[Notify],//null
                           options: Map[String, String],//map()
                           var steps: List[WorkflowStep]//steps
                         ) extends Formatable {
  def getProjectName(): String = Option(options).map(_.getOrElse("projectName", "default")).getOrElse("default")

  // scalastyle:off
  override def toString: String = {
    val builder = new StringBuilder()
    builder.append(headerStr)
    builder.append(steps.mkString("\n"))
    builder.toString()
  }

  def headerStr: String = {
    val builder = new StringBuilder()
    builder.append(s"-- workflow=$name$ENTER")
    if (!StringUtil.isNullOrEmpty(period)) builder.append(s"--  period=$period$ENTER")
    if (!StringUtil.isNullOrEmpty(loadType)) builder.append(s"--  loadType=$loadType$ENTER")
    if (!StringUtil.isNullOrEmpty(logDrivenType)) builder.append(s"--  logDrivenType=$logDrivenType$ENTER")
    if (!StringUtil.isNullOrEmpty(upstream)) builder.append(s"--  upstream=$upstream$ENTER")
    if (!StringUtil.isNullOrEmpty(dependsOn)) builder.append(s"--  dependsOn=$dependsOn$ENTER")
    if (!StringUtil.isNullOrEmpty(comment)) builder.append(s"--  comment=$comment$ENTER")
    if (!StringUtil.isNullOrEmpty(defaultStart)) builder.append(s"--  defaultStart=$defaultStart$ENTER")
    if (timeout > 1) builder.append(s"--  timeout=$timeout$ENTER")
    if (stopScheduleWhenFail) builder.append(s"--  stopScheduleWhenFail=$stopScheduleWhenFail$ENTER")
    if (notifies != null && notifies.nonEmpty) {
      notifies.foreach { notify =>
        builder.append(s"--  notify$ENTER")
        builder.append(s"--   notifyType=${notify.notifyType}$ENTER")
        builder.append(s"--   recipients=${notify.recipients}$ENTER")
        builder.append(s"--   notifyCondition=${notify.notifyCondition}$ENTER")
      }
    }
    builder.append(optionsToString)
    builder.append("\n")
    builder.toString()
  }

  def optionsToString: String = {
    if (options != null && options.nonEmpty) {
      val builder = new StringBuilder()
      builder.append(s"--  options$ENTER")
      options.foreach { case (key, value) => builder.append(s"--   $key=$value$ENTER") }
      builder.toString()
    } else {
      ""
    }
  }

  // scalastyle:on
}
