package com.github.sharpdata.sharpetl.spark.job

import com.github.sharpdata.sharpetl.spark.extension.UdfInitializer
import com.github.sharpdata.sharpetl.spark.utils.ETLSparkSession
import org.apache.spark.sql.SparkSession

object SparkSessionTestWrapper {
  lazy val spark: SparkSession = {
    ETLSparkSession.local = true
    val session = SparkSession
      .builder()
      .master("local")
      .appName("spark session")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .getOrCreate()
    UdfInitializer.init(session)
    session
  }
}

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = SparkSessionTestWrapper.spark

}
