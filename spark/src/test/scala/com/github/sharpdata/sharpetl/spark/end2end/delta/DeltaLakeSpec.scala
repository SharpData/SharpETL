package com.github.sharpdata.sharpetl.spark.end2end.delta

import com.github.sharpdata.sharpetl.core.util.ETLLogger
import com.github.sharpdata.sharpetl.spark.end2end.ETLSuit.runJob
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.DoNotDiscover

@DoNotDiscover
class DeltaLakeSpec extends DeltaSuit {

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
      getTimeStampFromStr("2022-02-02 17:12:59")
    )
  )

  lazy val firstDayDf = spark.createDataFrame(
    spark.sparkContext.parallelize(firstDayData),
    StructType(sourceSchema)
  )

  it("delta should works") {
    if (spark.version.startsWith("2.3") || spark.version.startsWith("3.5")) {
      ETLLogger.error("Delta Lake does NOT support Spark 2.3.x and Spark 3.5.x")
    } else if (spark.version.startsWith("2.4") || spark.version.startsWith("3.0") || spark.version.startsWith("3.1")) {
      ETLLogger.error("Delta Lake does not works well on Spark 2.4.x, " +
        "CREATE TABLE USING delta is not supported by Spark before 3.0.0 and Delta Lake before 0.7.0.")
    } else {
      runJob(source2odsParameters)

      val result = spark.sql("select * from delta_db.test_fact")
      assertSmallDataFrameEquality(result, firstDayDf, orderedComparison = false)
    }
  }
}
