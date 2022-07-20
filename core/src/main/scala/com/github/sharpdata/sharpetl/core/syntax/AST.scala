package com.github.sharpdata.sharpetl.core.syntax

import com.github.sharpdata.sharpetl.core.annotation.Annotations._
import com.github.sharpdata.sharpetl.core.notification.NotifyConfig

@Evolving(since = "1.0.0")
final case class Notify(notifyType: String, recipients: String, notifyCondition: String) {
  def toConfigs(): Seq[NotifyConfig] = {
    recipients
      .split(",")
      .map(_.trim)
      .map(recipient => NotifyConfig(notifyType, recipient, notifyCondition))
  }
}

