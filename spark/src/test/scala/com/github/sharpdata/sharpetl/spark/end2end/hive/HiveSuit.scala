package com.github.sharpdata.sharpetl.spark.end2end.hive

import com.github.sharpdata.sharpetl.spark.end2end.ETLSuit
import com.github.sharpdata.sharpetl.spark.utils.EmbeddedHive.sparkWithEmbeddedHive
import org.apache.spark.sql.SparkSession

trait HiveSuit extends ETLSuit {
  override lazy val spark: SparkSession = sparkWithEmbeddedHive
}
