package com.github.sharpdata.sharpetl.spark.end2end

import com.github.sharpdata.sharpetl.core.util.ETLLogger
import com.github.sharpdata.sharpetl.spark.end2end.ETLSuit.runJob
import com.github.sharpdata.sharpetl.spark.end2end.delta.DeltaSuit
import com.github.sharpdata.sharpetl.spark.test.DataFrameComparer
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should

import java.sql.Timestamp

class FlyDeltaSpec extends AnyFunSpec
  with should.Matchers
  with DeltaSuit
  //  with HiveSuit
  with DataFrameComparer
  with BeforeAndAfterEach {

  val data = Seq(
    Row("qqq", Timestamp.valueOf("2021-01-01 08:00:00")),
    Row("super qqq", Timestamp.valueOf("2021-01-01 08:00:00"))
  )

  val schema = List(
    StructField("name", StringType, true),
    StructField("update_time", TimestampType, true)
  )

  val sampleDataDf: DataFrame = spark.createDataFrame(
    spark.sparkContext.parallelize(data),
    StructType(schema)
  )

  val expected: DataFrame = spark.createDataFrame(
    spark.sparkContext.parallelize(
      Seq(
        Row("qqq", Timestamp.valueOf("2021-01-01 08:00:00"), 199),
        Row("super qqq", Timestamp.valueOf("2021-01-01 08:00:00"), 199)
      )
    ),
    StructType(List(
      StructField("new_name", StringType, true),
      StructField("update_time", TimestampType, true),
      StructField("test_expression", IntegerType, true)
    ))
  )

  it("should just run with delta") {
    if (spark.version.startsWith("2.3")) {
      ETLLogger.error("Delta Lake does NOT support Spark 2.3.x")
    } else if (spark.version.startsWith("2.4") || spark.version.startsWith("3.0") || spark.version.startsWith("3.1")) {
      ETLLogger.error("Delta Lake does not works well on Spark 2.4.x, " +
        "CREATE TABLE USING delta is not supported by Spark before 3.0.0 and Delta Lake before 0.7.0.")
    } else {
      val filePath = getClass.getResource("/application-delta.properties").toString

      val jobParameters: Array[String] = Array("single-job",
        "--name=latest-only",
        "--local", "--env=test", "--once", s"--property=$filePath")

      runJob(jobParameters)
    }
  }
}