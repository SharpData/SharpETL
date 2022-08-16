package com.github.sharpdata.sharpetl.spark.utils

import com.github.sharpdata.sharpetl.core.quality.DataQualityCheckResult
import org.apache.spark.sql.{Encoder, Encoders}

object Encoder extends Serializable {
  val dqEncoder: Encoder[DataQualityCheckResult] = Encoders.product[DataQualityCheckResult]
}
