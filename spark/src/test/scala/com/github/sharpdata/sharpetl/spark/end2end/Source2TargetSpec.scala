package com.github.sharpdata.sharpetl.spark.end2end

import com.github.sharpdata.sharpetl.core.util.DateUtil
import ETLSuit.runJob
import com.github.sharpdata.sharpetl.spark.end2end.mysql.MysqlSuit
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.DoNotDiscover

import java.sql.Timestamp
import java.time.LocalDateTime

@DoNotDiscover
class Source2TargetSpec extends MysqlSuit {

  override val createTableSql: String =
    "CREATE TABLE IF NOT EXISTS target" +
      " (id int, value varchar(255), bz_time timestamp, job_id varchar(255), job_time varchar(255));"

  override val sourceDbName: String = "int_test"
  val sourceTableName: String = "source"
  override val targetDbName: String = "int_test"

  val startTime = LocalDateTime.now().minusDays(1L).format(DateUtil.L_YYYY_MM_DD_HH_MM_SS)

  val jobParameters: Array[String] = Array("single-job",
    "--name=source_to_target", "--period=1440",
    "--local", s"--default-start-time=${startTime}", "--env=test")


  val schema = List(
    StructField("id", IntegerType, true),
    StructField("value", StringType, true),
    StructField("bz_time", TimestampType, true)
  )

  val time = Timestamp.valueOf(LocalDateTime.of(2021, 10, 1, 0, 0, 0))

  val data = Seq(
    Row(1, "111", time),
    Row(2, "222", time)
  )

  val sampleDataDf = spark.createDataFrame(
    spark.sparkContext.parallelize(data),
    StructType(schema)
  )


  it("simple source to target") {
    execute(createTableSql)
    writeDataToSource(sampleDataDf, sourceTableName)
    runJob(jobParameters)
    val resultDf = readFromSource("target")
    assertSmallDataFrameEquality(resultDf, sampleDataDf, orderedComparison = false)
  }
}
