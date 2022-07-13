package com.github.sharpdata.sharpetl.spark.end2end.hive

import com.github.sharpdata.sharpetl.spark.end2end.ETLSuit.runJob
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.scalatest.DoNotDiscover


@DoNotDiscover
class FactEventModelingSpec extends HiveSuit {

  val dwdSchema = List(
    StructField("event_id", StringType, true),
    StructField("event_status", StringType, true),
    StructField("create_time", TimestampType, true),
    StructField("update_time", TimestampType, true),
    StructField("year", StringType, true),
    StructField("month", StringType, true),
    StructField("day", StringType, true)
  )

  val filePath = getClass.getResource("/application-test.properties").toString

  it("Fact event, incremental & no SCD") {

    spark.sql("""create database if not exists ods""".stripMargin)
    spark.sql("""create database if not exists dwd""".stripMargin)
    spark.sql("""create database if not exists dim""".stripMargin)

    spark.sql(
      """
        |CREATE TABLE ods.t_event
        |(
        |    event_id string,
        |    device_IMEI string,
        |    device_model string,
        |    device_version string,
        |    device_language string,
        |    event_status string,
        |    create_time timestamp,
        |    update_time timestamp,
        |    job_id string
        |)
        |partitioned by (year string, month string, day string)""".stripMargin)

    spark.sql(
      """
        |CREATE TABLE dwd.t_fact_event
        |(
        |    event_id string,
        |    device_id string,
        |    event_status string,
        |    create_time timestamp,
        |    update_time timestamp,
        |    job_id  string
        |)
        |    partitioned by (year string, month string, day string)""".stripMargin)

    spark.sql(
      """
        |insert into dwd.t_fact_event partition (year = '2022', month = '03', day = '11')
        |values
        |("00001", "-1", "ONLINE", cast('2022-03-11 10:00:00' as timestamp), cast('2022-03-11 15:00:00' as timestamp), '1')
        |""".stripMargin)

    spark.sql(
      """
        |insert into dwd.t_fact_event partition (year = '2022', month = '03', day = '12')
        |values
        |("00002", "-1", "ONLINE", cast('2022-03-12 10:00:00' as timestamp), cast('2022-03-12 15:00:00' as timestamp), '1')
        |""".stripMargin)

    spark.sql(
      """
        |insert into ods.t_event partition (year = '2022', month = '03', day = '13')
        |values
        |("00001", "111", "iPhone14","16", "CHN", "ONLINE", cast('2022-03-11 10:00:00' as timestamp), cast('2022-03-13 15:00:00' as timestamp), '1'),
        |("00003", "333", "iPhone14 pro max", "18", "CHN", "ONLINE", cast('2022-03-13 10:00:00' as timestamp), cast('2022-03-13 15:00:00' as timestamp), '1')
        |""".stripMargin)

    spark.sql(
      """
        |CREATE TABLE dim.t_dim_device
        |(
        |    device_id          string,
        |    device_imei        string,
        |    device_model        string,
        |    device_version         string,
        |    device_language     string,
        |    create_time timestamp,
        |    update_time timestamp,
        |    job_id           string,
        |    start_time       timestamp,
        |    end_time         timestamp,
        |    is_latest        string,
        |    is_active        string,
        |    is_auto_created  string
        |)
        |partitioned by (year string, month string, day string)
        |""".stripMargin)

    runJob(Array("batch-job",
      "--names=fact_event", "--period=1440",
      "--default-start=20220313000000", "--log-driven-type=timewindow",
      "--env=embedded-hive", "--once", s"--property=$filePath"))

    val df = spark.sql("""select * from dwd.t_fact_event""")

    val data = Seq(
      Row("00001", "ONLINE", getTimeStampFromStr("2022-03-11 10:00:00"),
        getTimeStampFromStr("2022-03-13 15:00:00"), "2022", "03", "11"),
      Row("00002", "ONLINE", getTimeStampFromStr("2022-03-12 10:00:00"),
        getTimeStampFromStr("2022-03-12 15:00:00"), "2022", "03", "12"),
      Row("00003", "ONLINE", getTimeStampFromStr("2022-03-13 10:00:00"),
        getTimeStampFromStr("2022-03-13 15:00:00"), "2022", "03", "13")
    )

    val shouldBe = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      StructType(dwdSchema)
    )

    assertSmallDataFrameEquality(df.drop("device_id", "job_id"), shouldBe)
  }
}
