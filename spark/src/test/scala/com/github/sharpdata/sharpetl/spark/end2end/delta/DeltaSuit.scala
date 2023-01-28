package com.github.sharpdata.sharpetl.spark.end2end.delta

import com.github.sharpdata.sharpetl.spark.end2end.ETLSuit
import com.github.sharpdata.sharpetl.spark.extension.UdfInitializer
import com.github.sharpdata.sharpetl.spark.utils.ETLSparkSession
import org.apache.spark.sql.SparkSession

trait DeltaSuit extends ETLSuit {
  override lazy val spark: SparkSession = {
    ETLSparkSession.local = true
    val session = SparkSession
      .builder()
      .master("local")
      .appName("spark session")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()
    UdfInitializer.init(session)
    session
  }
}
