package com.github.sharpdata.sharpetl.core.quality

final case class QualityCheckRule(dataCheckType: String, rule: String, errorType: String) {
  def withColumn(column: String): DataQualityConfig = {
    if (rule.contains("$")) {
      DataQualityConfig(column, dataCheckType, rule.replace("$column", s"`$column`"), errorType)
    } else {
      DataQualityConfig(column, dataCheckType, rule, errorType)
    }
  }
}
