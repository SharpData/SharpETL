package com.github.sharpdata.sharpetl.spark.end2end

import ETLSuit.runJob
import com.github.sharpdata.sharpetl.core.util.DateUtil
import com.github.sharpdata.sharpetl.spark.end2end.mysql.MysqlSuit
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.DoNotDiscover

import java.sql.Timestamp
import java.time.LocalDateTime

@DoNotDiscover
class BatchJobSpec extends MysqlSuit {
  override val createTableSql: String = ""
  override val targetDbName = "int_test"
  override val sourceDbName: String = "int_test"

  val data = Seq(
    Row("jiale", Timestamp.valueOf("2021-01-01 08:00:00")),
    Row("super jiale", Timestamp.valueOf("2021-01-01 08:00:00"))
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
        Row("jiale", Timestamp.valueOf("2021-01-01 08:00:00"), 199),
        Row("super jiale", Timestamp.valueOf("2021-01-01 08:00:00"), 199)
      )
    ),
    StructType(List(
      StructField("new_name", StringType, true),
      StructField("update_time", TimestampType, true),
      StructField("test_expression", IntegerType, true)
    ))
  )

  it("should only the latest job only") {
    val now = LocalDateTime.now()
    val startTime = now.minusDays(4L).format(DateUtil.L_YYYY_MM_DD_HH_MM_SS)
    val dataRangeStart = now.minusDays(1L).format(DateUtil.INT_YYYY_MM_DD_HH_MM_SS)

    val jobParameters: Array[String] = Array("batch-job",
      "--names=latest-only", "--period=1440",
      "--local", s"--default-start-time=${startTime}", "--env=test", "--latest-only")

    runJob(jobParameters)

    val df = readFromLog("job_log").where("workflow_name = 'latest-only'")
    df.count() should be(1)
    df.select("data_range_start").head().get(0) should be(dataRangeStart)
  }

  it("should refresh time-based job") {
    val startTime = LocalDateTime.now().minusDays(4L).format(DateUtil.L_YYYY_MM_DD_HH_MM_SS)
    val refreshStart = LocalDateTime.now().minusDays(2L).format(DateUtil.INT_YYYY_MM_DD_HH_MM_SS)
    val refreshEnd = LocalDateTime.now().minusDays(1L).format(DateUtil.INT_YYYY_MM_DD_HH_MM_SS)

    val jobParameters: Array[String] = Array("single-job",
      "--name=refresh-temp", "--period=1440",
      "--local", s"--default-start-time=${startTime}", "--env=test", "--once")

    runJob(jobParameters)

    readFromLog("job_log").where("workflow_name = 'refresh-temp'").count() should be(1)

    val newJobParameters: Array[String] = Array("single-job",
      "--name=refresh-temp", "--period=1440", "--refresh", s"--refresh-range-start=${refreshStart}", s"--refresh-range-end=${refreshEnd}",
      "--local", "--env=test")

    runJob(newJobParameters)

    readFromLog("job_log").where("workflow_name = 'refresh-temp'").count() should be(2)
  }

  it("should skip if already done") {
    runJob(Array("batch-job",
      s"--names=refresh-temp", "--period=1440",
      "--local",
      "--env=test", "--parallelism=1"))

    readFromLog("job_log").where("workflow_name = 'refresh-temp'").count() should be(3)

    runJob(Array("batch-job",
      s"--names=refresh-temp", "--period=1440",
      "--local",
      "--env=test", "--parallelism=1"))

    readFromLog("job_log").where("workflow_name = 'refresh-temp'").count() should be(3)
  }

  it("should from user defined step id") {
    runJob(Array("single-job",
      s"--name=from_step_id", "--period=1440",
      "--local",
      "--env=test", "--once", "--from-step=2"))
  }

  it("should exclude from user defined step id") {
    runJob(Array("single-job",
      s"--name=from_step_id", "--period=1440",
      "--local",
      "--env=test", "--once", "--exclude-steps=1"))
  }

  it("should run hello world successfully") {
    runJob(Array("single-job",
      s"--name=hello_world", "--period=1440",
      "--local",
      "--env=test", "--once"))
  }
}
