package com.github.sharpdata.sharpetl.core.syntax

import com.google.common.base.Strings.isNullOrEmpty
import com.github.sharpdata.sharpetl.core.annotation.Annotations.{Evolving, Stable}
import com.github.sharpdata.sharpetl.core.util.Constants.Separator.ENTER
import com.github.sharpdata.sharpetl.core.util.StringUtil

@Evolving(since = "1.0.0")
final case class Notify(notifyType: String, recipients: String, notifyCondition: String)


@Evolving(since = "1.0.0")
final case class Workflow(
                           name: String,
                           period: String,
                           loadType: String,
                           logDrivenType: String,
                           upstream: String,
                           dependsOn: String,
                           comment: String,
                           timeout: Int,
                           defaultStart: String,
                           stopScheduleWhenFail: Boolean,
                           notification: Notify,
                           options: Map[String, String],
                           var steps: List[WorkflowStep]
                         ) extends Formatable {
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
    if (notification != null && !isNullOrEmpty(notification.notifyType)) {
      builder.append(s"--  notification$ENTER")
      builder.append(s"--   notifyType=${notification.notifyType}$ENTER")
      builder.append(s"--   recipients=${notification.recipients}$ENTER")
      builder.append(s"--   notifyCondition=${notification.notifyCondition}$ENTER")
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
