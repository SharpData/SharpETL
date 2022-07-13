package com.github.sharpdata.sharpetl.core.quality

import com.github.sharpdata.sharpetl.core.annotation.Annotations.Stable

@Stable(since = "1.0.0")
trait UserDefinedRule extends Serializable {
  def check(tempViewName: String, idColumn: String, udr: DataQualityConfig): (String, String)
}

object UserDefinedRule extends Serializable {
  val PREFIX = "UDR"
}
