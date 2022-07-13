package com.github.sharpdata.sharpetl.core.quality

object ErrorType extends Serializable {
  val warn = "warn"
  val error = "error"
}

final case class DataQualityConfig(column: String, dataCheckType: String, rule: String, errorType: String)

final case class DataQualityCheckResult(column: String, dataCheckType: String, ids: String, errorType: String, warnCount: Long, errorCount: Long)
