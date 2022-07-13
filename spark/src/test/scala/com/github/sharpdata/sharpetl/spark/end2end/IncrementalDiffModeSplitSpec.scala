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
class IncrementalDiffModeSplitSpec extends ETLSuit {

  override val createTableSql: String =
    """CREATE TABLE IF NOT EXISTS test_split
      |(id int, user_id varchar(255), user_name varchar(255),
      | user_account varchar(255), bz_time DATETIME,
      | dt varchar(255), job_id varchar(255),
      | job_time varchar(255)
      |);""".stripMargin

  override val sourceDbName: String = "int_test"
  val sourceTableName: String = "test_fact_split_source"
  override val targetDbName: String = "int_test"

  val firstDay = "2021-10-01 00:00:00"
  val secondDay = "2021-10-02 00:00:00"

  val source2odsParameters: Array[String] = Array("single-job",
    "--name=test_fact_split_source", "--period=1440",
    "--local", s"--default-start-time=${firstDay}", "--env=test", "--once")

  val ods2dwdParameters: Array[String] = Array("single-job",
    "--name=test_fact_split", "--period=1440",
    "--local", s"--default-start-time=${firstDay}", "--env=test", "--once")


  val sourceSchema = List(
    StructField("id", IntegerType, true),
    StructField("user_id", StringType, true),
    StructField("user_name", StringType, true),
    StructField("user_account", StringType, true),
    StructField("bz_time", TimestampType, true)
  )

  val dimSchema = List(
    StructField("id", StringType, true),
    StructField("user_name", StringType, true),
    StructField("user_account", StringType, true)
  )

  val dimData = List(
    Row("user_123", "123", "acc_123"),
    Row("user_233", "233", "acc_233"),
    Row("user_666", "666", "acc_666"),
    Row("user_123", "123", "acc_123"),
    Row("user_777", "777", "acc_777"),
    Row("user_555", "555", "acc_555"),
    Row("user_6", "6", "acc_6")
  )

  val dimDf = spark.createDataFrame(
    spark.sparkContext.parallelize(dimData),
    StructType(dimSchema)
  )

  val firstDayData = Seq(
    Row(1, "user_123", "123", "acc_123", getTimeStampFromStr("2021-10-07 17:12:59")),
    Row(2, "user_233", "233", "acc_233", getTimeStampFromStr("2021-10-08 17:12:59")),
    Row(3, "user_666", "666", "acc_666", getTimeStampFromStr("2021-10-08 17:12:59")),
    Row(4, "user_777", "777", "acc_777", getTimeStampFromStr("2021-10-11 17:12:59")),
    Row(5, "no exist user", "???", "???", getTimeStampFromStr("2021-10-11 17:12:59")) // dim not match
  )

  val firstDayDf = spark.createDataFrame(
    spark.sparkContext.parallelize(firstDayData),
    StructType(sourceSchema)
  )

  val secondDayData = Seq(
    //Row(1, "user_123", "123", "acc-123", getTimeStampFromStr("2021-10-07 17:12:59")), deleted
    Row(2, "user_666", "666", "acc_666", getTimeStampFromStr("2021-10-09 17:12:59")), // updated dim
    Row(3, "user_666", "666", "acc_666", getTimeStampFromStr("2021-10-08 17:12:59")), // no change
    Row(4, "no exist user", "???", "???", getTimeStampFromStr("2021-10-12 17:12:59")), // updated but dim not match
    Row(5, "user_555", "555", "acc_555", getTimeStampFromStr("2021-10-12 17:12:59")), // update dim matched
    Row(6, "user_6", "6", "acc_6", getTimeStampFromStr("2021-10-11 17:12:59")) // added
  )

  val secondDayDf = spark.createDataFrame(
    spark.sparkContext.parallelize(secondDayData),
    StructType(sourceSchema)
  )

  val dwdSchema = List(
    StructField("id", IntegerType, true),
    StructField("user_id", StringType, true),
    StructField("bz_time", TimestampType, true),
    //StructField("effective_start_time", TimestampType, true),
    //StructField("effective_end_time", TimestampType, true),
    StructField("is_latest", StringType, true),
    StructField("is_active", StringType, true)
  )

  val expectedData = Seq(
    //old
    Row(1, "user_123", getTimeStampFromStr("2021-10-07 17:12:59"), "1", "1"),
    Row(2, "user_233", getTimeStampFromStr("2021-10-08 17:12:59"), "1", "1"),
    Row(3, "user_666", getTimeStampFromStr("2021-10-08 17:12:59"), "1", "1"),
    Row(4, "user_777", getTimeStampFromStr("2021-10-11 17:12:59"), "1", "1"),
    Row(5, "-1", getTimeStampFromStr("2021-10-11 17:12:59"), "1", "1"), // not match!
    //new
    Row(1, "user_123", getTimeStampFromStr("2021-10-07 17:12:59"), "1", "0"), //deleted
    Row(2, "user_233", getTimeStampFromStr("2021-10-08 17:12:59"), "0", "0"), //updated
    Row(2, "user_666", getTimeStampFromStr("2021-10-09 17:12:59"), "1", "1"), //updated
    Row(3, "user_666", getTimeStampFromStr("2021-10-08 17:12:59"), "1", "1"), // no change
    Row(4, "user_777", getTimeStampFromStr("2021-10-11 17:12:59"), "0", "0"), //updated
    Row(4, "-1", getTimeStampFromStr("2021-10-12 17:12:59"), "1", "1"), // updated dim not match
    Row(5, "-1", getTimeStampFromStr("2021-10-11 17:12:59"), "0", "0"),
    Row(5, "user_555", getTimeStampFromStr("2021-10-12 17:12:59"), "1", "1"), //updated dim matched
    Row(6, "user_6", getTimeStampFromStr("2021-10-11 17:12:59"), "1", "1") //added
  )

  val expectedDf = spark.createDataFrame(
    spark.sparkContext.parallelize(expectedData),
    StructType(dwdSchema)
  )


  it("incremental update copy") {
    execute(createTableSql)
    execute(
      """
        |CREATE TABLE IF NOT EXISTS test_fact_split
        |(
        |    id                   int,
        |    user_id              varchar(64),
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
    writeDataToSource(dimDf, "test_user")
    //1. source => ods
    runJob(source2odsParameters)
    //2. ods => dwd
    runJob(ods2dwdParameters)
    val resultDf = readFromTarget("test_split").drop("dt")
    assertSmallDataFrameEquality(resultDf, firstDayDf, orderedComparison = false)
    //3. update/delete/insert data
    execute("DROP TABLE test_fact_split_source")
    writeDataToSource(secondDayDf, sourceTableName)
    //4. source => ods
    runJob(source2odsParameters)
    //val secondResultDf = readFromTarget("test_ods").where(s"dt = '$secondDay'").drop("dt")
    //assertSmallDataFrameEquality(secondResultDf, secondDayDf)
    //5. ods => dwd
    runJob(ods2dwdParameters)
    // 6. check zip table
    val dwdDf = readFromTarget("test_fact_split").drop("idempotent_key", "dw_insert_date", "effective_start_time", "effective_end_time")
    assertSmallDataFrameEquality(dwdDf, expectedDf, orderedComparison = false)
  }
}
