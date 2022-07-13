package com.github.sharpdata.sharpetl.spark.end2end.hive

import com.github.sharpdata.sharpetl.spark.end2end.ETLSuit.runJob
import com.github.sharpdata.sharpetl.datasource.kafka.DFConversations._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.scalatest.DoNotDiscover


@DoNotDiscover
class DimStudentModelingSpec extends HiveSuit {

  val dwdSchema = List(
    StructField("student_code", StringType, true),
    StructField("student_name", StringType, true),
    StructField("student_age", StringType, true),
    StructField("student_address", StringType, true),
    StructField("student_create_time", TimestampType, true),
    StructField("student_update_time", TimestampType, true),
    StructField("start_time", TimestampType, true),
    StructField("end_time", TimestampType, true),
    StructField("is_latest", StringType, true),
    StructField("is_active", StringType, true),
    StructField("is_auto_created", StringType, true),
    StructField("year", StringType, true),
    StructField("month", StringType, true),
    StructField("day", StringType, true)
  )

  val filePath = getClass.getResource("/application-test.properties").toString

  it("[DIM&INC] new data only") {

    spark.sql("""create database if not exists ods""".stripMargin)
    spark.sql("""create database if not exists dim""".stripMargin)

    spark.sql(
      """
        |CREATE TABLE ods.t_student
        |(
        |    student_code        string,
        |    student_name        string,
        |    student_age         string,
        |    student_address     string,
        |    student_blabla      string,
        |    student_create_time timestamp,
        |    student_update_time timestamp
        |)
        |    partitioned by (year string, month string, day string)""".stripMargin)

    //new created user
    spark.sql(
      """
        |insert into ods.t_student partition (year = '2022', month = '03', day = '13')
        |values ("zhang san", "user name", "18", "user address", "blabala", cast('2022-03-13 10:00:00' as timestamp), cast('2022-03-13 15:00:00' as timestamp))
        |""".stripMargin)

    spark.sql(
      """
        |CREATE TABLE dim.t_dim_student
        |(
        |    student_id          string,
        |    student_code        string,
        |    student_name        string,
        |    student_age         string,
        |    student_address     string,
        |    student_create_time timestamp,
        |    student_update_time timestamp,
        |    job_id           string,
        |    start_time       timestamp,
        |    end_time         timestamp,
        |    is_latest        string,
        |    is_active        string,
        |    is_auto_created  string
        |)
        |    partitioned by (year string, month string, day string)
        |""".stripMargin)

    runJob(Array("batch-job",
      "--names=dim_student", "--period=1440",
      "--default-start=20220313000000", "--log-driven-type=timewindow",
      "--env=embedded-hive", "--once", s"--property=$filePath"))

    val df = spark.sql("""select * from dim.t_dim_student""")

    val newCreatedData = Seq(
      Row("zhang san", "user name", "18", "user address", getTimeStampFromStr("2022-03-13 10:00:00"),
        getTimeStampFromStr("2022-03-13 15:00:00"), getTimeStampFromStr("2022-03-13 10:00:00"),
        null, "1", "1", "0", "2022", "03", "13"
      )
    )

    val shouldBe = spark.createDataFrame(
      spark.sparkContext.parallelize(newCreatedData),
      StructType(dwdSchema)
    )

    assertSmallDataFrameEquality(df.drop("student_id", "job_id"), shouldBe)
  }

  it("[DIM&INC] new data with updated data") {
    spark.sql(
      """
        |insert into ods.t_student partition (year = '2022', month = '03', day = '14')
        |values ("zhang san", "new user name", "19", "new user address", "blabala", cast('2022-03-13 15:00:00' as timestamp), cast('2022-03-14 18:00:00' as timestamp)),
        |       ("li si", "li si si li", "20", "lisi user address", "blabala", cast('2022-03-14 11:00:00' as timestamp), cast('2022-03-14 11:00:00' as timestamp))
        |""".stripMargin)

    runJob(Array("batch-job",
      "--names=dim_student", "--period=1440",
      "--default-start=20220313000000", "--log-driven-type=timewindow",
      "--env=embedded-hive", "--once", s"--property=$filePath"))

    val df = spark.sql("""select * from dim.t_dim_student""")

    val newCreatedData = Seq(
      Row("zhang san", "user name", "18", "user address", getTimeStampFromStr("2022-03-13 10:00:00"),
        getTimeStampFromStr("2022-03-13 15:00:00"), getTimeStampFromStr("2022-03-13 10:00:00"),
        getTimeStampFromStr("2022-03-14 18:00:00"), "0", "0", "0", "2022", "03", "13"
      ), Row("zhang san", "new user name", "19", "new user address", getTimeStampFromStr("2022-03-13 15:00:00"),
        getTimeStampFromStr("2022-03-14 18:00:00"), getTimeStampFromStr("2022-03-14 18:00:00"),
        null, "1", "1", "0", "2022", "03", "13"
      ), Row("li si", "li si si li", "20", "lisi user address", getTimeStampFromStr("2022-03-14 11:00:00"),
        getTimeStampFromStr("2022-03-14 11:00:00"), getTimeStampFromStr("2022-03-14 11:00:00"),
        null, "1", "1", "0", "2022", "03", "14"
      )
    )

    val shouldBe = spark.createDataFrame(
      spark.sparkContext.parallelize(newCreatedData),
      StructType(dwdSchema)
    )

    assertSmallDataFrameEquality(df.drop("student_id", "job_id"), shouldBe, orderedComparison = false)

    assertSmallDataFrameEquality(spark.sql("select job_id from dim.t_dim_student where year = '2022' and month = '03' and day = '13' limit 1"),
      spark.sql("select job_id from dim.t_dim_student where year = '2022' and month = '03' and day = '14' limit 1"))
  }

  it("[DIM&INC] new data with updated data (should keep old partition unchanged)") {
    spark.sql(
      """
        |insert into ods.t_student partition (year = '2022', month = '03', day = '15')
        |values ("zhang san", "another new user name", "19", "new user address", "blabala", cast('2022-03-13 15:00:00' as timestamp), cast('2022-03-15 15:00:00' as timestamp)),
        |       ("wang wu", "li si si li", "20", "lisi user address", "blabala", cast('2022-03-15 11:00:00' as timestamp), cast('2022-03-15 11:00:00' as timestamp))
        |""".stripMargin)

    runJob(Array("batch-job",
      "--names=dim_student", "--period=1440",
      "--default-start=20220313000000", "--log-driven-type=timewindow",
      "--env=embedded-hive", "--once", s"--property=$filePath"))

    val df = spark.sql("""select * from dim.t_dim_student""")

    val newCreatedData = Seq(
      Row("zhang san", "user name", "18", "user address", getTimeStampFromStr("2022-03-13 10:00:00"),
        getTimeStampFromStr("2022-03-13 15:00:00"), getTimeStampFromStr("2022-03-13 10:00:00"),
        getTimeStampFromStr("2022-03-14 18:00:00"), "0", "0", "0", "2022", "03", "13"
      ), Row("zhang san", "new user name", "19", "new user address", getTimeStampFromStr("2022-03-13 15:00:00"),
        getTimeStampFromStr("2022-03-14 18:00:00"), getTimeStampFromStr("2022-03-14 18:00:00"),
        getTimeStampFromStr("2022-03-15 15:00:00"), "0", "0", "0", "2022", "03", "13"
      ), Row("zhang san", "another new user name", "19", "new user address", getTimeStampFromStr("2022-03-13 15:00:00"),
        getTimeStampFromStr("2022-03-15 15:00:00"), getTimeStampFromStr("2022-03-15 15:00:00"),
        null, "1", "1", "0", "2022", "03", "13"
      ), Row("li si", "li si si li", "20", "lisi user address", getTimeStampFromStr("2022-03-14 11:00:00"),
        getTimeStampFromStr("2022-03-14 11:00:00"), getTimeStampFromStr("2022-03-14 11:00:00"),
        null, "1", "1", "0", "2022", "03", "14"
      ), Row("wang wu", "li si si li", "20", "lisi user address", getTimeStampFromStr("2022-03-15 11:00:00"),
        getTimeStampFromStr("2022-03-15 11:00:00"), getTimeStampFromStr("2022-03-15 11:00:00"),
        null, "1", "1", "0", "2022", "03", "15"
      )
    )

    val shouldBe = spark.createDataFrame(
      spark.sparkContext.parallelize(newCreatedData),
      StructType(dwdSchema)
    )

    assertSmallDataFrameEquality(df.drop("student_id", "job_id"), shouldBe, orderedComparison = false)

    assertSmallDataFrameEquality(spark.sql("select job_id from dim.t_dim_student where year = '2022' and month = '03' and day = '13' limit 1"),
      spark.sql("select job_id from dim.t_dim_student where year = '2022' and month = '03' and day = '15' limit 1"))
  }
}
