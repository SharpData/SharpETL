package com.github.sharpdata.sharpetl.spark.end2end

import com.github.sharpdata.sharpetl.core.util.ETLLogger
import ETLSuit.runJob
import com.github.sharpdata.sharpetl.core.util.ETLConfig.deltaLakeBasePath
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.DoNotDiscover

@DoNotDiscover
class DeltaLakeSpec extends ETLSuit {

  override val createTableSql: String = ""
  override val sourceDbName: String = "int_test"
  val sourceTableName: String = "test_delta_table"
  override val targetDbName: String = "int_test"

  val firstDay = "2021-10-01 00:00:00"
  val secondDay = "2021-10-02 00:00:00"

  val source2odsParameters: Array[String] = Array("single-job",
    "--name=test_auto_create_dim_source_delta", "--period=1440",
    "--local", s"--default-start-time=${firstDay}", "--env=test", "--once")

  val sourceSchema = List(
    StructField("id", StringType, true),
    StructField("bz_time", TimestampType, true)
  )

  val firstDayData = Seq(
    Row("1",
      getTimeStampFromStr("2021-10-07 17:12:59")
    ),
    Row("2",
      getTimeStampFromStr("2021-10-07 17:12:59")
    ),
    Row("3",
      getTimeStampFromStr("2021-10-07 17:12:59")
    )
  )

  val firstDayDf = spark.createDataFrame(
    spark.sparkContext.parallelize(firstDayData),
    StructType(sourceSchema)
  )

  it("delta should works") {
    if (spark.version.startsWith("2.3") || spark.version.startsWith("3.3")) {
      ETLLogger.error("Delta Lake does NOT support Spark 2.3.x or 3.3.x")
    } else {
      writeDataToSource(firstDayDf, sourceTableName)
      runJob(source2odsParameters)

      val result = spark.read.format("delta").load(s"$deltaLakeBasePath/test_fact").drop("job_id", "job_time")
      assertSmallDataFrameEquality(result, firstDayDf, orderedComparison = false)
    }
  }
}
