package com.github.sharpdata.sharpetl.spark.end2end

import ETLSuit.runJob
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.DoNotDiscover
import org.scalatest.matchers.should

/**
 * 1. source => ods
 * 2. ods => dwd
 * 3. error data 不应该出现在结果里面
 * 4. 没有错误的和warn的应该出现在结果中
 */
@DoNotDiscover
class DataQualityCheckSpec extends ETLSuit with should.Matchers {

  override val createTableSql: String =
    "CREATE TABLE IF NOT EXISTS test_ods_for_quality_check" +
      " (order_id int, phone varchar(255), value varchar(255), bz_time DATETIME, dt varchar(255), job_id varchar(255), job_time varchar(255));"

  override val sourceDbName: String = "int_test"
  val sourceTableName: String = "test_source_for_quality_check"
  override val targetDbName: String = "int_test"

  val firstDay = "2021-10-01 00:00:00"

  val source2odsParameters: Array[String] = Array("single-job",
    "--name=test_source_for_quality_check", "--period=1440",
    "--local", s"--default-start-time=${firstDay}", "--env=test", "--once")

  val ods2dwdParameters: Array[String] = Array("single-job",
    "--name=test_dwd_with_quality_check", "--period=1440",
    "--local", s"--default-start-time=${firstDay}", "--env=test", "--once")


  val schema = List(
    StructField("order_id", IntegerType, true),
    StructField("phone", StringType, true),
    StructField("value", StringType, true),
    StructField("bz_time", TimestampType, true)
  )

  val firstDayData = Seq(
    Row(1, "110", "2333", getTimeStampFromStr("2021-10-07 17:12:59")), // normal
    Row(2, null, "aba aba", getTimeStampFromStr("2021-10-08 17:12:59")), // error
    Row(3, "110", "", getTimeStampFromStr("2021-10-08 17:12:59")) //warn
  )

  val firstDayDf = spark.createDataFrame(
    spark.sparkContext.parallelize(firstDayData),
    StructType(schema)
  )

  val dwdSchema = List(
    StructField("order_id", IntegerType, true),
    StructField("phone", StringType, true),
    StructField("value", StringType, true),
    StructField("bz_time", TimestampType, true),
    //StructField("effective_start_time", TimestampType, true),
    //StructField("effective_end_time", TimestampType, true),
    StructField("is_latest", StringType, true),
    StructField("is_active", StringType, true)
  )

  val expectedData = Seq(
    Row(1, "110", "2333", getTimeStampFromStr("2021-10-07 17:12:59"), "1", "1"), //normal
    //Row(2, null, "aba aba", getTimeStampFromStr("2021-10-08 17:12:59"), "1", "1"), //error
    Row(3, "110", "", getTimeStampFromStr("2021-10-08 17:12:59"), "1", "1") //warn
  )

  val expectedDf = spark.createDataFrame(
    spark.sparkContext.parallelize(expectedData),
    StructType(dwdSchema)
  )

  val qualityCheckSchema = List(
    StructField("job_id", IntegerType, true),
    StructField("job_schedule_id", StringType, true),
    StructField("column", StringType, true),
    StructField("data_check_type", StringType, true),
    StructField("ids", IntegerType, true),
    StructField("error_type", StringType, true),
    StructField("warn_count", IntegerType, true),
    StructField("error_count", IntegerType, true)
  )

  val expectedQualityCheck = Seq(
    Row(2, "test_dwd_with_quality_check-20211001000000", "phone", "power null check(error)", 2, "error", 0, 1),
    Row(2, "test_dwd_with_quality_check-20211001000000", "value", "empty check(warn)", 3, "warn", 1, 0)
  )

  val expectedQualityCheckDf = spark.createDataFrame(
    spark.sparkContext.parallelize(expectedQualityCheck),
    StructType(qualityCheckSchema)
  )

  it("error data should not sink to dwd, warn & normal data could") {
    execute(createTableSql)
    execute(
      """
        |CREATE TABLE IF NOT EXISTS test_dwd_for_quality_check
        |(
        |    order_id             int,
        |    phone                varchar(64),
        |    value                varchar(64),
        |    bz_time              DATETIME NULL DEFAULT NULL,
        |    job_id               varchar(64),
        |    job_time             DATETIME NULL DEFAULT NULL,
        |    effective_start_time DATETIME NULL DEFAULT NULL,
        |    effective_end_time   DATETIME NULL DEFAULT NULL,
        |    is_latest            varchar(64),
        |    is_active            varchar(64),
        |    idempotent_key       varchar(64),
        |    dw_insert_date       varchar(64)
        |)
        |""".stripMargin)
    writeDataToSource(firstDayDf, sourceTableName)
    //1. source => ods
    runJob(source2odsParameters)
    //2. ods => dwd
    runJob(ods2dwdParameters)
    val resultDf = readFromTarget("test_ods_for_quality_check").drop("dt")
    assertSmallDataFrameEquality(resultDf, firstDayDf, orderedComparison = false)

    // 3. check data
    val dwdDf = readFromTarget("test_dwd_for_quality_check").drop("idempotent_key", "dw_insert_date", "effective_start_time", "effective_end_time")
    assertSmallDataFrameEquality(dwdDf, expectedDf, orderedComparison = false)
    // 4. 检查 quality_check_log table
    val qualityCheckDf =
      spark.read.format("jdbc")
        .option("url", "jdbc:mysql://localhost:2333/sharp_etl")
        .option("user", "admin").option("password", "admin").option("dbtable", "quality_check_log")
        .load()
        .drop("id", "create_time", "last_update_time")
    val qualityCheckList =
      qualityCheckDf.select("job_schedule_id", "column", "data_check_type", "ids", "error_type", "warn_count", "error_count")
        .rdd.toLocalIterator.toSeq.map(_.toString())
    val expectedQualityCheckList =
      expectedQualityCheckDf.select("job_schedule_id", "column", "data_check_type", "ids", "error_type", "warn_count", "error_count")
        .rdd.toLocalIterator.toSeq.map(_.toString())
    qualityCheckList should contain theSameElementsAs expectedQualityCheckList
  }

}
