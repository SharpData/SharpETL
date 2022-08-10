package com.github.sharpdata.sharpetl.spark.end2end.hive

import com.github.sharpdata.sharpetl.spark.end2end.ETLSuit.runJob
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.DoNotDiscover


@DoNotDiscover
class AutoCreateDimSpec extends HiveSuit {

  val productSchema = List(
    StructField("mid", StringType, true),
    StructField("name", StringType, true),
    StructField("product_version", StringType, true),
    StructField("product_status", StringType, true),
    StructField("start_time", TimestampType, true),
    StructField("end_time", TimestampType, true),
    StructField("is_latest", StringType, true),
    StructField("is_active", StringType, true),
    StructField("is_auto_created", StringType, true),
    StructField("year", StringType, true),
    StructField("month", StringType, true),
    StructField("day", StringType, true)
  )

  val classSchema = List(
    StructField("class_code", StringType, true),
    StructField("class_name", StringType, true),
    StructField("class_address", StringType, true),
    StructField("start_time", TimestampType, true),
    StructField("end_time", TimestampType, true),
    StructField("is_latest", StringType, true),
    StructField("is_active", StringType, true),
    StructField("is_auto_created", StringType, true),
    StructField("year", StringType, true),
    StructField("month", StringType, true),
    StructField("day", StringType, true)
  )

  val orderSchema = List(
    StructField("order_sn", StringType, true),
    StructField("product_id", StringType, true),
    StructField("user_id", StringType, true),
    StructField("class_id", StringType, true),
    StructField("product_count", StringType, true),
    StructField("price", DoubleType, true),
    StructField("discount", DoubleType, true),
    StructField("order_status", StringType, true),
    StructField("order_create_time", TimestampType, true),
    StructField("order_update_time", TimestampType, true),
    StructField("actual", StringType, true),
    StructField("job_id", StringType, true),
    StructField("start_time", TimestampType, true),
    StructField("end_time", TimestampType, true),
    StructField("is_latest", StringType, true),
    StructField("is_active", StringType, true),
    StructField("year", StringType, true),
    StructField("month", StringType, true),
    StructField("day", StringType, true)
  )

  val filePath = getClass.getResource("/application-test.properties").toString

  it("[ALWAYS & ONCE & NEVER] create dim") {
    spark.sql("""create database if not exists ods""".stripMargin)
    spark.sql("""create database if not exists dim""".stripMargin)
    spark.sql("""create database if not exists dwd""".stripMargin)
    spark.sql(
      """
        |CREATE TABLE ods.t_order
        |(
        |    order_id           string,
        |    order_sn           string,
        |    product_code        string,
        |    product_name         string,
        |    product_version     string,
        |    product_status      string,
        |    user_code      string,
        |    user_name      string,
        |    user_age      int,
        |    user_address      string,
        |    class_code      string,
        |    class_name      string,
        |    class_address      string,
        |    product_count      int,
        |    price             double,
        |    discount          double,
        |    order_status      string,
        |    order_create_time timestamp,
        |    order_update_time timestamp
        |)
        |    partitioned by (year string, month string, day string)""".stripMargin)


    spark.sql(
      """
        |CREATE TABLE dwd.t_fact_order
        |(
        |    order_id           string,
        |    order_sn           string,
        |    product_id         string,
        |    user_id            string,
        |    class_id           string,
        |    product_count      string,
        |    price              double,
        |    discount           double,
        |    order_status       string,
        |    order_create_time  timestamp,
        |    order_update_time  timestamp,
        |    actual             double,
        |    job_id             string,
        |    start_time         timestamp,
        |    end_time           timestamp,
        |    is_latest          string,
        |    is_active          string
        |)
        |    partitioned by (year string, month string, day string)""".stripMargin)

    spark.sql(
      """
        |insert into ods.t_order partition (year = '2022', month = '03', day = '13')
        |values ("o_2022_03_13_01", "sn_o_2022_03_13_01", "p_001", "new product name", "v1", "good",
        |         "user_code_01", "zhangsan", 18, "Mars",
        |         "0708", "高三一班", "Mars",
        |         2, 23.33, 6.66, "created",
        |         cast('2022-03-13 09:00:00' as timestamp), cast('2022-03-13 11:00:00' as timestamp)),
        |      ("o_2022_03_13_01","sn_o_2022_03_13_01", "p_001", "old product name", "v1", "good",
        |         "user_code_01", "zhangsan", 17, "Mars",
        |         "0708", "高三一班", "Mars",
        |         2, 23.33, 6.66, "created",
        |         cast('2022-03-13 09:00:00' as timestamp), cast('2022-03-13 09:00:00' as timestamp)),
        |      ("o_2022_03_13_02", "sn_o_2022_03_13_02", "p_002", "product name2", "v1", "bad",
        |         "user_code_02", "lisi", 17, "Mars",
        |         "0709", "高三二班", "Mars",
        |         2, 23.33, 6.66, "created",
        |         cast('2022-03-13 15:00:00' as timestamp), cast('2022-03-13 15:00:00' as timestamp)),
        |      ("o_2022_03_13_04", "sn_o_2022_03_13_04", "p_003", "product name3", "v1", "bad",
        |         "user_code_02", "lisi", 17, "Mars",
        |         "0709", "高三二班", "Mars",
        |         2, 23.33, 6.66, "created",
        |         cast('2022-03-13 09:00:00' as timestamp), cast('2022-03-13 15:00:00' as timestamp)),
        |      ("o_2022_03_13_05", "sn_o_2022_03_13_05", "p_004", "new product name4", "v1", "bad",
        |        "user_code_02", "wangwu", 19, "Mars",
        |         "0709", "高三二班", "Mars",
        |         2, 23.33, 6.66, "created",
        |         cast('2022-03-13 09:00:00' as timestamp), cast('2022-03-13 15:00:00' as timestamp))
        |""".stripMargin)


    spark.sql(
      """
        |CREATE TABLE dim.t_dim_class
        |(
        |    class_id string,
        |    class_code string,
        |    class_name string,
        |    class_address string,
        |    job_id           string,
        |    start_time       timestamp,
        |    end_time         timestamp,
        |    is_latest        string,
        |    is_active        string,
        |    is_auto_created  string
        |)
        |    partitioned by (year string, month string, day string)
        |""".stripMargin)

    spark.sql(
      """
        |insert into dim.t_dim_class partition (year = '2022', month = '03', day = '13')
        |values ("123 123 123", "0708", "高三一班", "Moon",
        |'1', cast('2022-03-10 15:00:00' as timestamp), null, '1', '1', '0')
        |""".stripMargin)


    spark.sql(
      """
        |CREATE TABLE dim.t_dim_product
        |(
        |    product_id          string,
        |    mid        string,
        |    name        string,
        |    product_version         string,
        |    product_status     string,
        |    job_id           string,
        |    start_time       timestamp,
        |    end_time         timestamp,
        |    is_latest        string,
        |    is_active        string,
        |    is_auto_created  string
        |)
        |    partitioned by (year string, month string, day string)
        |""".stripMargin)

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS dim.t_dim_user
        |(
        |    dim_user_id          string,
        |    user_info_code  string,
        |    user_name string,
        |    user_age integer,
        |    user_address  string,
        |    job_id           string,
        |    start_time       timestamp,
        |    end_time         timestamp,
        |    is_latest        string,
        |    is_active        string,
        |    is_auto_created  string
        |)
        |    partitioned by (year string, month string, day string)
        |""".stripMargin)

    spark.sql(
      """
        |insert into dim.t_dim_product partition (year = '2022', month = '03', day = '13')
        |values ("33333333333", "p_003", "product name3", "v1", "bad",
        |'1', cast('2022-03-13 09:00:00' as timestamp), null, '1', '1', '0'),
        |("44444444", "p_004", "product name4", "v1", "bad",
        |'1', cast('2022-03-13 09:00:00' as timestamp), null, '1', '1', '0')
        |""".stripMargin)


    runJob(Array("batch-job",
      "--names=auto_create_dim", "--period=1440",
      "--default-start=20220313000000", "--log-driven-type=timewindow",
      "--env=embedded-hive", "--once", s"--property=$filePath"))

    val productDf = spark.sql("""select * from dim.t_dim_product""".stripMargin)
    //productDf.show(100, truncate = false)

    val newCreatedData = Seq(
      Row("p_003", "product name3", "v1", "bad", getTimeStampFromStr("2022-03-13 09:00:00"),
        null, "1", "1", "0", "2022", "03", "13"
      ),
      Row("p_004", "product name4", "v1", "bad", getTimeStampFromStr("2022-03-13 09:00:00"),
        getTimeStampFromStr("2022-03-13 15:00:00"), "0", "0", "0", "2022", "03", "13"
      ),
      Row("p_001", "old product name", "v1", "good", getTimeStampFromStr("2022-03-13 09:00:00"),
        getTimeStampFromStr("2022-03-13 11:00:00"), "0", "0", "1", "2022", "03", "13"
      ),
      Row("p_001", "new product name", "v1", "good", getTimeStampFromStr("2022-03-13 11:00:00"),
        null, "1", "1", "1", "2022", "03", "13"
      ),
      Row("p_002", "product name2", "v1", "bad", getTimeStampFromStr("2022-03-13 15:00:00"),
        null, "1", "1", "1", "2022", "03", "13"
      ),
      Row("p_004", "new product name4", "v1", "bad", getTimeStampFromStr("2022-03-13 15:00:00"),
        null, "1", "1", "1", "2022", "03", "13"
      )
    )

    val productShouldBe = spark.createDataFrame(
      spark.sparkContext.parallelize(newCreatedData),
      StructType(productSchema)
    )

    assertSmallDataFrameEquality(productDf.drop("product_id", "job_id"), productShouldBe, orderedComparison = false)


    val classDf = spark.sql("""select * from dim.t_dim_class""".stripMargin)
    //classDf.show(100, truncate = false)

    val classData = Seq(
      Row("0708", "高三一班", "Moon", getTimeStampFromStr("2022-03-10 15:00:00"),
        null, "1", "1", "0", "2022", "03", "13"
      ), Row("0709", "高三二班", "Mars", getTimeStampFromStr("2022-03-13 15:00:00"),
        null, "1", "1", "1", "2022", "03", "13"
      )
    )

    val classShouldBe = spark.createDataFrame(
      spark.sparkContext.parallelize(classData),
      StructType(classSchema)
    )

    assertSmallDataFrameEquality(classDf.drop("class_id", "job_id"), classShouldBe, orderedComparison = false)
  }
}
