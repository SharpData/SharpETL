package com.github.sharpdata.sharpetl.spark.utils

import com.github.sharpdata.sharpetl.core.quality.{CheckResult, DataQualityCheckResult}
import org.apache.spark.sql.{DataFrame, Encoder, Encoders}

final class SparkCheckResult(override val warn: Seq[DataQualityCheckResult],
                             override val error: Seq[DataQualityCheckResult],
                             override val passed: DataFrame)
  extends CheckResult(warn, error, passed)

object Encoder extends Serializable {
  val dqEncoder: Encoder[DataQualityCheckResult] = Encoders.product[DataQualityCheckResult]
}
