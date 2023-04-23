package com.github.sharpdata.sharpetl.spark.end2end

import com.github.sharpdata.sharpetl.spark.end2end.ETLSuit.runJob
import com.github.sharpdata.sharpetl.spark.end2end.delta.DeltaSuit
import com.github.sharpdata.sharpetl.spark.end2end.hive.HiveSuit
import com.github.sharpdata.sharpetl.spark.job.SparkSessionTestWrapper
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
    val filePath = getClass.getResource("/application-delta.properties").toString

    val jobParameters: Array[String] = Array("single-job",
      "--name=latest-only",
      "--local", "--env=test", "--once", s"--property=$filePath")

    runJob(jobParameters)
  }
}
