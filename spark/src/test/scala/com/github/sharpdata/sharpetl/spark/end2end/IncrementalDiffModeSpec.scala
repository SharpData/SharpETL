package com.github.sharpdata.sharpetl.spark.end2end

import ETLSuit.runJob
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.DoNotDiscover

/**
 * 1. source => ods
 * 2. ods => dwd
 * 3. update/delete/insert data
 * 4. source => ods
 * 5. ods => dwd
 * 6. check zip table
 */
@DoNotDiscover
class IncrementalDiffModeSpec extends ETLSuit {

   override val createTableSql: String =
    "CREATE TABLE IF NOT EXISTS test_ods" +
      " (order_id int, value varchar(255), bz_time DATETIME, dt varchar(255), job_id varchar(255), job_time varchar(255));"

  override val sourceDbName: String = "int_test"
  val sourceTableName: String = "test_source"
  override val targetDbName: String = "int_test"

  val firstDay = "2021-10-01 00:00:00"
  val secondDay = "2021-10-02 00:00:00"

  val source2odsParameters: Array[String] = Array("single-job",
    "--name=test_source", "--period=1440",
    "--local", s"--default-start-time=${firstDay}", "--env=test", "--once")

  val ods2dwdParameters: Array[String] = Array("single-job",
    "--name=test_dwd", "--period=1440",
    "--local", s"--default-start-time=${firstDay}", "--env=test", "--once")


  val schema = List(
    StructField("order_id", IntegerType, true),
    StructField("value", StringType, true),
    StructField("bz_time", TimestampType, true)
  )

  val firstDayData = Seq(
    Row(1, "2333", getTimeStampFromStr("2021-10-07 17:12:59")),
    Row(2, "aba aba", getTimeStampFromStr("2021-10-08 17:12:59")),
    Row(3, "qwer", getTimeStampFromStr("2021-10-08 17:12:59"))
  )

  val firstDayDf = spark.createDataFrame(
    spark.sparkContext.parallelize(firstDayData),
    StructType(schema)
  )

  val secondDayData = Seq(
    //Row(1, "2333", getTimeStampFromStr("2021-10-07 17:12:59")), deleted
    Row(2, "lllllll", getTimeStampFromStr("2021-10-09 17:12:59")), // updated
    Row(3, "qwer", getTimeStampFromStr("2021-10-08 17:12:59")), // no change
    Row(4, "edaaa", getTimeStampFromStr("2021-10-11 17:12:59")), // added
    Row(5, "55555", getTimeStampFromStr("2021-10-11 17:12:59")) // added
  )

  val secondDayDf = spark.createDataFrame(
    spark.sparkContext.parallelize(secondDayData),
    StructType(schema)
  )

  val dwdSchema = List(
    StructField("order_id", IntegerType, true),
    StructField("value", StringType, true),
    StructField("bz_time", TimestampType, true),
    //StructField("effective_start_time", TimestampType, true),
    //StructField("effective_end_time", TimestampType, true),
    StructField("is_latest", StringType, true),
    StructField("is_active", StringType, true)
  )

  val expectedData = Seq(
    //old
    Row(1, "2333", getTimeStampFromStr("2021-10-07 17:12:59"), "1", "1"),
    Row(2, "aba aba", getTimeStampFromStr("2021-10-08 17:12:59"), "1", "1"),
    Row(3, "qwer", getTimeStampFromStr("2021-10-08 17:12:59"), "1", "1"),
    //new
    Row(4, "edaaa", getTimeStampFromStr("2021-10-11 17:12:59"), "1", "1"), //added
    Row(1, "2333", getTimeStampFromStr("2021-10-07 17:12:59"), "1", "0"), //deleted
    Row(5, "55555", getTimeStampFromStr("2021-10-11 17:12:59"), "1", "1"), //added
    Row(2, "lllllll", getTimeStampFromStr("2021-10-09 17:12:59"), "1", "1"), //no change
    Row(2, "aba aba", getTimeStampFromStr("2021-10-08 17:12:59"), "0", "0"), //updated
    Row(3, "qwer", getTimeStampFromStr("2021-10-08 17:12:59"), "1", "1") //no change
  )

  val expectedDf = spark.createDataFrame(
    spark.sparkContext.parallelize(expectedData),
    StructType(dwdSchema)
  )


  it("incremental update copy") {
    execute(createTableSql)
    execute(
      """
        |CREATE TABLE IF NOT EXISTS test_dwd
        |(
        |    order_id             int,
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
    val resultDf = readFromTarget("test_ods").drop("dt")
    assertSmallDataFrameEquality(resultDf, firstDayDf, orderedComparison = false)
    //3. update/delete/insert data
    execute("DROP TABLE test_source")
    writeDataToSource(secondDayDf, sourceTableName)
    //4. source => ods
    runJob(source2odsParameters)
    //val secondResultDf = readFromTarget("test_ods").where(s"dt = '$secondDay'").drop("dt")
    //assertSmallDataFrameEquality(secondResultDf, secondDayDf)
    //5. ods => dwd
    runJob(ods2dwdParameters)
    // 6. check zip table
    val dwdDf = readFromTarget("test_dwd").drop("idempotent_key", "dw_insert_date", "effective_start_time", "effective_end_time")
    assertSmallDataFrameEquality(dwdDf, expectedDf, orderedComparison = false)
  }


}
