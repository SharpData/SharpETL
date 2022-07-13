package com.github.sharpdata.sharpetl.spark.end2end.postgres

import com.github.sharpdata.sharpetl.spark.end2end.ETLSuit.runJob
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types._
import org.scalatest.DoNotDiscover


@DoNotDiscover
class PostgresModelingSpec extends PostgresEtlSuit {

  val odsOrderSchema = List(
    StructField("order_sn", StringType, true),
    StructField("product_code", StringType, true),
    StructField("product_name", StringType, true),
    StructField("product_version", StringType, true),
    StructField("product_status", StringType, true),
    StructField("user_code", StringType, true),
    StructField("user_name", StringType, true),
    StructField("user_age", IntegerType, true),
    StructField("user_address", StringType, true),
    StructField("product_count", IntegerType, true),
    StructField("price", DecimalType(10, 4), true),
    StructField("discount", DecimalType(10, 4), true),
    StructField("order_status", StringType, true),
    StructField("order_create_time", TimestampType, true),
    StructField("order_update_time", TimestampType, true)
  )

  val dwdOrderNoSCDSchema = List(
    StructField("order_sn", StringType, true),
    StructField("product_id", StringType, true),
    StructField("user_id", StringType, true),
    StructField("product_count", IntegerType, true),
    StructField("price", DecimalType(10, 4), true),
    StructField("discount", DecimalType(10, 4), true),
    StructField("order_status", StringType, true),
    StructField("order_create_time", TimestampType, true),
    StructField("order_update_time", TimestampType, true),
    StructField("actual", DecimalType(10, 4), true)
  )

  val dwdOrderSCDSchema = List(
    StructField("order_sn", StringType, true),
    StructField("product_id", StringType, true),
    StructField("user_id", StringType, true),
    StructField("product_count", IntegerType, true),
    StructField("price", DecimalType(10, 4), true),
    StructField("discount", DecimalType(10, 4), true),
    StructField("order_status", StringType, true),
    StructField("order_create_time", TimestampType, true),
    StructField("order_update_time", TimestampType, true),
    StructField("actual", DecimalType(10, 4), true),
    StructField("start_time", TimestampType, true),
    StructField("end_time", TimestampType, true),
    StructField("is_active", StringType, true),
    StructField("is_latest", StringType, true)
  )

  val filePath = getClass.getResource("/application-test.properties").toString

  it("source -> ods test") {
    val createSchemaSql =
      """
        |CREATE SCHEMA IF NOT EXISTS sales;
        |CREATE SCHEMA IF NOT EXISTS ods;""".stripMargin
    execute(createSchemaSql, "postgres", 5432)

    val createSourceTableSql =
      """
        |drop table if exists sales.order;
        |create table if not exists sales.order
        |(
        |    order_sn          varchar(128),
        |    product_code      varchar(128),
        |    product_name      varchar(128),
        |    product_version   varchar(128),
        |    product_status    varchar(128),
        |    user_code         varchar(128),
        |    user_name         varchar(128),
        |    user_age          int,
        |    user_address      varchar(128),
        |    product_count     int,
        |    price             decimal(10, 4),
        |    discount          decimal(10, 4),
        |    order_status      varchar(128),
        |    order_create_time timestamp,
        |    order_update_time timestamp
        |);""".stripMargin
    execute(createSourceTableSql, "postgres", 5432)

    val initSourceDataSql =
      """
        |insert into sales.order (order_sn, product_code, product_name, product_version, product_status, user_code, user_name, user_age, user_address, product_count, price, discount, order_status, order_create_time, order_update_time)
        |values ('AAA', 'p1', '华为', 'mate40', '上架', 'u1', '张三', 12, '胜利街道', 12, 20, 0.3, 2, '2022-04-04 10:00:00', '2022-04-08 10:00:00')
        |     , ('BBB', 'p1', '华为', 'mate40', '上架', 'u1', '张三', 12, '胜利街道', 12, 10, 0.3, 1, '2022-04-04 10:00:00', '2022-04-08 10:00:00')
        |     , ('DDD', 'p1', '华为', 'mate40-v2', '上架', 'u2', '李四', 32, '迎宾街道', 15, 200, 0.4, 1, '2022-04-08 09:00:00', '2022-04-08 10:00:00');""".stripMargin
    execute(initSourceDataSql, "postgres", 5432)

    val createOdsTableSql =
      """
        |drop table if exists ods.t_order;
        |create table if not exists ods.t_order
        |(
        |    order_sn          varchar(128),
        |    product_code      varchar(128),
        |    product_name      varchar(128),
        |    product_version   varchar(128),
        |    product_status    varchar(128),
        |    user_code         varchar(128),
        |    user_name         varchar(128),
        |    user_age          int,
        |    user_address      varchar(128),
        |    product_count     int,
        |    price             decimal(10, 4),
        |    discount          decimal(10, 4),
        |    order_status      varchar(128),
        |    order_create_time timestamp,
        |    order_update_time timestamp,
        |    job_id            varchar(128)
        |) ;""".stripMargin
    execute(createOdsTableSql, "postgres", 5432)

    runJob(Array("single-job",
      s"--name=source_to_ods", "--period=1440",
      s"--default-start-time=2022-04-08 00:00:00", "--once", "--local", s"--property=$filePath"))

    val odsDf = readTable("postgres", 5432, "ods.t_order")
    odsDf.show()
    val newCreatedData = Seq(
      Row("AAA", "p1", "华为", "mate40", "上架", "u1", "张三", 12, "胜利街道", 12, BigDecimal(20.0000), BigDecimal(0.3000), "2", getTimeStampFromStr("2022-04-04 10:00:00"), getTimeStampFromStr("2022-04-08 10:00:00")),
      Row("BBB", "p1", "华为", "mate40", "上架", "u1", "张三", 12, "胜利街道", 12, BigDecimal(10.0000), BigDecimal(0.3000), "1", getTimeStampFromStr("2022-04-04 10:00:00"), getTimeStampFromStr("2022-04-08 10:00:00")),
      Row("DDD", "p1", "华为", "mate40-v2", "上架", "u2", "李四", 32, "迎宾街道", 15, BigDecimal(200.0000), BigDecimal(0.4000), "1", getTimeStampFromStr("2022-04-08 09:00:00"), getTimeStampFromStr("2022-04-08 10:00:00"))
    )
    val odsOrderShouldBe = spark.createDataFrame(
      spark.sparkContext.parallelize(newCreatedData),
      StructType(odsOrderSchema)
    )
    odsOrderShouldBe.show()

    assertSmallDataFrameEquality(odsDf.drop("job_id"), odsOrderShouldBe, orderedComparison = false)

  }

  it("ods -> dwd full & no SCD") {
    val createSchemaSql =
      """
        |CREATE SCHEMA IF NOT EXISTS ods;
        |CREATE SCHEMA IF NOT EXISTS dwd;
        |create extension if not exists "uuid-ossp";""".stripMargin
    execute(createSchemaSql, "postgres", 5432)

    val createOdsTableSql =
      """
        |drop table if exists ods.t_order;
        |create table if not exists ods.t_order
        |(
        |    order_sn          varchar(128),
        |    product_code      varchar(128),
        |    product_name      varchar(128),
        |    product_version   varchar(128),
        |    product_status    varchar(128),
        |    user_code         varchar(128),
        |    user_name         varchar(128),
        |    user_age          int,
        |    user_address      varchar(128),
        |    product_count     int,
        |    price             decimal(10, 4),
        |    discount          decimal(10, 4),
        |    order_status      varchar(128),
        |    order_create_time timestamp,
        |    order_update_time timestamp,
        |    job_id            varchar(128)
        |) ;""".stripMargin
    execute(createOdsTableSql, "postgres", 5432)

    val initOdsSql =
      """
        |insert into ods.t_order (job_id, order_sn, product_code, product_name, product_version, product_status, user_code, user_name, user_age, user_address, product_count, price, discount, order_status, order_create_time, order_update_time) values
        | (1, 'AAA', 'p1', '华为', 'mate40', '上架', 'u1', '张三', 12, '迎宾街道', 12, 20, 0.3, 2, '2022-04-04 10:00:00', '2022-04-08 10:00:00') -- 正常更新
        |,(1, 'BBB', 'p1', '华为', 'mate40', '上架', 'u1', '张三', 12, '迎宾街道', 12, 30, 0.3, 1, '2022-04-04 11:00:00', '2022-04-08 09:00:00') -- 迟到时间，不做更新，该状态还是1，不更新
        |,(1, 'DDD', 'p1', '华为', 'mate40', '上架', 'u1', '张三', 12, '迎宾街道', 12, 200, 0.4, 1, '2022-04-04 12:00:00', '2022-04-08 12:00:00') -- 新增
        |,(2, 'AAA', 'p1', '华为', 'mate40', '上架', 'u1', '张三', 12, '迎宾街道', 12, 20, 0.3, 2, '2022-04-04 10:00:00', '2022-04-08 10:00:00') -- 正常更新
        |,(2, 'BBB', 'p1', '华为', 'mate40', '上架', 'u1', '张三', 12, '迎宾街道', 12, 30, 0.3, 1, '2022-04-04 10:00:00', '2022-04-08 09:00:00') -- 迟到时间，不做更新，该状态还是1，不更新
        |,(2, 'DDD', 'p1', '华为', 'mate40', '上架', 'u1', '张三', 12, '迎宾街道', 12, 200, 0.4, 2, '2022-04-04 10:00:00', '2022-04-09 10:00:00') -- 状态更新
        |,(2, 'EEE', 'p1', '华为', 'mate40', '上架', 'u1', '张三', 12, '迎宾街道', 12, 200, 1.4, 1, '2022-04-09 10:00:00', '2022-04-09 10:00:00'); -- 新增""".stripMargin
    execute(initOdsSql, "postgres", 5432)

    val createDwdTableSql =
      """
        |drop table if exists dwd.t_fact_order;
        |create table dwd.t_fact_order(
        |    order_sn	varchar(128),
        |    product_id	varchar(128),
        |    user_id	varchar(128),
        |    product_count	int,
        |    price	decimal(10,4),
        |    discount	decimal(10,4),
        |    order_status	varchar(128),
        |    order_create_time	timestamp,
        |    order_update_time	timestamp,
        |    actual	decimal(10,4)
        |);
        |
        |drop table if exists dwd.t_dim_product;
        |create table dwd.t_dim_product(
        |	id  varchar(128) default uuid_generate_v1(), -- 渐变id
        |    mid varchar(128),
        |    name varchar(128),
        |    version varchar(128),
        |    status varchar(128),
        |    create_time timestamp,
        |    update_time timestamp,
        |    start_time timestamp,
        |    end_time timestamp,
        |    is_active varchar(1),
        |    is_latest varchar(1),
        |    is_auto_created varchar(1)
        |);
        |
        |drop table if exists dwd.t_dim_user;
        |create table dwd.t_dim_user(
        |	user_id  varchar(128) default uuid_generate_v1(), -- 渐变id
        |    user_code varchar(128),
        |    user_name varchar(128),
        |    user_age int,
        |    user_address varchar(128),
        |    create_time timestamp,
        |    update_time timestamp,
        |    start_time timestamp,
        |    end_time timestamp,
        |    is_active varchar(1),
        |    is_latest varchar(1),
        |    is_auto_created varchar(1)
        |);""".stripMargin
    execute(createDwdTableSql, "postgres", 5432)

    val initDwdSql =
      """
        |insert into dwd.t_dim_product(id, mid, name, version, status, create_time, update_time, start_time, end_time, is_active, is_latest, is_auto_created) values
        | ('3abd0495-9abe-44a0-b95b-0e42aeadc807', 'p1', '华为', 'mate40', '上架', '2021-01-01 10:00:00', '2021-01-01 10:00:00', '2021-01-01 10:00:00', null, '1', '1', '0');
        |
        |insert into dwd.t_dim_user(user_id, user_code, user_name, user_age, user_address, create_time, update_time, start_time, end_time, is_active, is_latest, is_auto_created) values
        |('06347be1-f752-4228-8480-4528a2166e14', 'u1', '张三', 12, '胜利街道', '2020-01-01 10:00:00', '2020-01-01 10:00:00', '2020-01-01 10:00:00', null, '1', '1', '0');
        |
        |insert into dwd.t_fact_order(order_sn, product_id, user_id, product_count, price, discount, order_status, order_create_time, order_update_time, actual) values
        |('AAA', '3abd0495-9abe-44a0-b95b-0e42aeadc807', '06347be1-f752-4228-8480-4528a2166e14', 12, 20, 0.3, 1, '2022-04-04 10:00:00', '2022-04-04 10:00:00', 19.7),
        |('BBB', '3abd0495-9abe-44a0-b95b-0e42aeadc807', '06347be1-f752-4228-8480-4528a2166e14', 12, 10, 0.3, 2, '2022-04-04 11:00:00', '2022-04-08 10:00:00', 9.7),
        |('CCC', '3abd0495-9abe-44a0-b95b-0e42aeadc807', '06347be1-f752-4228-8480-4528a2166e14', 12, 30, 0.3, 2, '2022-04-04 12:00:00', '2022-04-07 10:00:00', 29.7);
        |""".stripMargin
    execute(initDwdSql, "postgres", 5432)

    val truncateOdsLogSql =
      """
        |truncate table job_log;""".stripMargin
    executeMigration(truncateOdsLogSql)

    val truncateOdsLogStepSql =
      """
        |truncate table step_log;""".stripMargin
    executeMigration(truncateOdsLogStepSql)

    val initOdsLogSql =
      """
        |insert into job_log (job_id, job_name, job_period, job_schedule_id, data_range_start, data_range_end, status) values
        |(1, 'source_to_ods', 1440, 'source_to_ods-20220408000000', '20220408000000', '20220409000000', 'SUCCESS'),
        |(2, 'source_to_ods', 1440, 'source_to_ods-20220408000000', '20220408000000', '20220409000000', 'SUCCESS');
        |""".stripMargin
    executeMigration(initOdsLogSql)

    runJob(Array("single-job",
      s"--name=ods_to_dwd_full_no_sc", "--period=1440",
      "--local", s"--property=$filePath"))

    val dwdDf = readTable("postgres", 5432, "dwd.t_fact_order")
    dwdDf.show()
    val newCreatedData = Seq(
      Row("BBB", "3abd0495-9abe-44a0-b95b-0e42aeadc807", "06347be1-f752-4228-8480-4528a2166e14", 12, BigDecimal(10.0000), BigDecimal(0.3000), "2", getTimeStampFromStr("2022-04-04 11:00:00"), getTimeStampFromStr("2022-04-08 10:00:00"), BigDecimal(9.7)),
      Row("AAA", "3abd0495-9abe-44a0-b95b-0e42aeadc807", "06347be1-f752-4228-8480-4528a2166e14", 12, BigDecimal(20.0000), BigDecimal(0.3000), "2", getTimeStampFromStr("2022-04-04 10:00:00"), getTimeStampFromStr("2022-04-08 10:00:00"), BigDecimal(19.7)),
      Row("DDD", "3abd0495-9abe-44a0-b95b-0e42aeadc807", "06347be1-f752-4228-8480-4528a2166e14", 12, BigDecimal(200.0000), BigDecimal(0.4000), "2", getTimeStampFromStr("2022-04-04 10:00:00"), getTimeStampFromStr("2022-04-09 10:00:00"), BigDecimal(199.6)),
      Row("EEE", "3abd0495-9abe-44a0-b95b-0e42aeadc807", "06347be1-f752-4228-8480-4528a2166e14", 12, BigDecimal(200.0000), BigDecimal(1.4000), "1", getTimeStampFromStr("2022-04-09 10:00:00"), getTimeStampFromStr("2022-04-09 10:00:00"), BigDecimal(198.6))
    )
    val dwdOrderShouldBe = spark.createDataFrame(
      spark.sparkContext.parallelize(newCreatedData),
      StructType(dwdOrderNoSCDSchema)
    )
    dwdOrderShouldBe.show()

    assertSmallDataFrameEquality(dwdDf.drop("job_id"), dwdOrderShouldBe, orderedComparison = false)

  }

  it("ods -> dwd incremental & no SCD") {
    val createSchemaSql =
      """
        |CREATE SCHEMA IF NOT EXISTS ods;
        |CREATE SCHEMA IF NOT EXISTS dwd;
        |create extension if not exists "uuid-ossp";""".stripMargin
    execute(createSchemaSql, "postgres", 5432)

    val createOdsTableSql =
      """
        |drop table if exists ods.t_order;
        |create table if not exists ods.t_order
        |(
        |    order_sn          varchar(128),
        |    product_code      varchar(128),
        |    product_name      varchar(128),
        |    product_version   varchar(128),
        |    product_status    varchar(128),
        |    user_code         varchar(128),
        |    user_name         varchar(128),
        |    user_age          int,
        |    user_address      varchar(128),
        |    product_count     int,
        |    price             decimal(10, 4),
        |    discount          decimal(10, 4),
        |    order_status      varchar(128),
        |    order_create_time timestamp,
        |    order_update_time timestamp,
        |    job_id            varchar(128)
        |) ;""".stripMargin
    execute(createOdsTableSql, "postgres", 5432)

    val initOdsSql =
      """
        |insert into ods.t_order (job_id, order_sn, product_code, product_name, product_version, product_status, user_code, user_name, user_age, user_address, product_count, price, discount, order_status, order_create_time, order_update_time) values
        | (1, 'AAA', 'p1', '华为', 'mate40', '上架', 'u1', '张三', 12, '迎宾街道', 12, 20, 0.3, 2, '2022-04-04 10:00:00', '2022-04-08 10:00:00') -- 正常更新
        |,(1, 'BBB', 'p1', '华为', 'mate40', '上架', 'u1', '张三', 12, '迎宾街道', 12, 30, 0.3, 1, '2022-04-04 11:00:00', '2022-04-08 09:00:00') -- 迟到时间，不做更新，该状态还是1，不更新
        |,(1, 'DDD', 'p1', '华为', 'mate40', '上架', 'u1', '张三', 12, '迎宾街道', 12, 200, 0.4, 1, '2022-04-04 12:00:00', '2022-04-08 12:00:00') -- 新增
        |,(2, 'DDD', 'p1', '华为', 'mate40', '上架', 'u1', '张三', 12, '迎宾街道', 12, 200, 0.4, 2, '2022-04-04 10:00:00', '2022-04-09 10:00:00') -- 状态更新
        |,(2, 'EEE', 'p1', '华为', 'mate40', '上架', 'u1', '张三', 12, '迎宾街道', 12, 200, 1.4, 1, '2022-04-09 10:00:00', '2022-04-09 10:00:00'); -- 新增""".stripMargin
    execute(initOdsSql, "postgres", 5432)

    val createDwdTableSql =
      """
        |drop table if exists dwd.t_fact_order;
        |create table dwd.t_fact_order(
        |    order_sn	varchar(128),
        |    product_id	varchar(128),
        |    user_id	varchar(128),
        |    product_count	int,
        |    price	decimal(10,4),
        |    discount	decimal(10,4),
        |    order_status	varchar(128),
        |    order_create_time	timestamp,
        |    order_update_time	timestamp,
        |    actual	decimal(10,4)
        |);
        |
        |drop table if exists dwd.t_dim_product;
        |create table dwd.t_dim_product(
        |	id  varchar(128) default uuid_generate_v1(), -- 渐变id
        |    mid varchar(128),
        |    name varchar(128),
        |    version varchar(128),
        |    status varchar(128),
        |    create_time timestamp,
        |    update_time timestamp,
        |    start_time timestamp,
        |    end_time timestamp,
        |    is_active varchar(1),
        |    is_latest varchar(1),
        |    is_auto_created varchar(1)
        |);
        |
        |drop table if exists dwd.t_dim_user;
        |create table dwd.t_dim_user(
        |	user_id  varchar(128) default uuid_generate_v1(), -- 渐变id
        |    user_code varchar(128),
        |    user_name varchar(128),
        |    user_age int,
        |    user_address varchar(128),
        |    create_time timestamp,
        |    update_time timestamp,
        |    start_time timestamp,
        |    end_time timestamp,
        |    is_active varchar(1),
        |    is_latest varchar(1),
        |    is_auto_created varchar(1)
        |);""".stripMargin
    execute(createDwdTableSql, "postgres", 5432)

    val initDwdSql =
      """
        |insert into dwd.t_dim_product(id, mid, name, version, status, create_time, update_time, start_time, end_time, is_active, is_latest, is_auto_created) values
        | ('3abd0495-9abe-44a0-b95b-0e42aeadc807', 'p1', '华为', 'mate40', '上架', '2021-01-01 10:00:00', '2021-01-01 10:00:00', '2021-01-01 10:00:00', null, '1', '1', '0');
        |
        |insert into dwd.t_dim_user(user_id, user_code, user_name, user_age, user_address, create_time, update_time, start_time, end_time, is_active, is_latest, is_auto_created) values
        |('06347be1-f752-4228-8480-4528a2166e14', 'u1', '张三', 12, '胜利街道', '2020-01-01 10:00:00', '2020-01-01 10:00:00', '2020-01-01 10:00:00', null, '1', '1', '0');
        |
        |insert into dwd.t_fact_order(order_sn, product_id, user_id, product_count, price, discount, order_status, order_create_time, order_update_time, actual) values
        |('AAA', '3abd0495-9abe-44a0-b95b-0e42aeadc807', '06347be1-f752-4228-8480-4528a2166e14', 12, 20, 0.3, 1, '2022-04-04 10:00:00', '2022-04-04 10:00:00', 19.7),
        |('BBB', '3abd0495-9abe-44a0-b95b-0e42aeadc807', '06347be1-f752-4228-8480-4528a2166e14', 12, 10, 0.3, 2, '2022-04-04 11:00:00', '2022-04-08 10:00:00', 9.7),
        |('CCC', '3abd0495-9abe-44a0-b95b-0e42aeadc807', '06347be1-f752-4228-8480-4528a2166e14', 12, 30, 0.3, 2, '2022-04-04 12:00:00', '2022-04-07 10:00:00', 29.7);
        |""".stripMargin
    execute(initDwdSql, "postgres", 5432)

    val truncateOdsLogSql =
      """
        |truncate table job_log;""".stripMargin
    executeMigration(truncateOdsLogSql)

    val truncateOdsLogStepSql =
      """
        |truncate table step_log;""".stripMargin
    executeMigration(truncateOdsLogStepSql)

    val initOdsLogSql =
      """
        |insert into job_log (job_id, job_name, job_period, job_schedule_id, data_range_start, data_range_end, status) values
        |(1, 'source_to_ods', 1440, 'source_to_ods-20220408000000', '20220408000000', '20220409000000', 'SUCCESS'),
        |(2, 'source_to_ods', 1440, 'source_to_ods-20220408000000', '20220408000000', '20220409000000', 'SUCCESS');
        |""".stripMargin
    executeMigration(initOdsLogSql)

    runJob(Array("single-job",
      s"--name=ods_to_dwd_incremental_no_sc", "--period=1440",
      "--local", s"--property=$filePath"))

    val dwdDf = readTable("postgres", 5432, "dwd.t_fact_order")
    dwdDf.show()
    val newCreatedData = Seq(
      Row("BBB", "3abd0495-9abe-44a0-b95b-0e42aeadc807", "06347be1-f752-4228-8480-4528a2166e14", 12, BigDecimal(10.0000), BigDecimal(0.3000), "2", getTimeStampFromStr("2022-04-04 11:00:00"), getTimeStampFromStr("2022-04-08 10:00:00"), BigDecimal(9.7)),
      Row("CCC", "3abd0495-9abe-44a0-b95b-0e42aeadc807", "06347be1-f752-4228-8480-4528a2166e14", 12, BigDecimal(30.0000), BigDecimal(0.3000), "2", getTimeStampFromStr("2022-04-04 12:00:00"), getTimeStampFromStr("2022-04-07 10:00:00"), BigDecimal(29.7)),
      Row("AAA", "3abd0495-9abe-44a0-b95b-0e42aeadc807", "06347be1-f752-4228-8480-4528a2166e14", 12, BigDecimal(20.0000), BigDecimal(0.3000), "2", getTimeStampFromStr("2022-04-04 10:00:00"), getTimeStampFromStr("2022-04-08 10:00:00"), BigDecimal(19.7)),
      Row("DDD", "3abd0495-9abe-44a0-b95b-0e42aeadc807", "06347be1-f752-4228-8480-4528a2166e14", 12, BigDecimal(200.0000), BigDecimal(0.4000), "2", getTimeStampFromStr("2022-04-04 10:00:00"), getTimeStampFromStr("2022-04-09 10:00:00"), BigDecimal(199.6)),
      Row("EEE", "3abd0495-9abe-44a0-b95b-0e42aeadc807", "06347be1-f752-4228-8480-4528a2166e14", 12, BigDecimal(200.0000), BigDecimal(1.4000), "1", getTimeStampFromStr("2022-04-09 10:00:00"), getTimeStampFromStr("2022-04-09 10:00:00"), BigDecimal(198.6))
    )
    val dwdOrderShouldBe = spark.createDataFrame(
      spark.sparkContext.parallelize(newCreatedData),
      StructType(dwdOrderNoSCDSchema)
    )
    dwdOrderShouldBe.show()

    assertSmallDataFrameEquality(dwdDf.drop("job_id"), dwdOrderShouldBe, orderedComparison = false)

  }
  it("ods -> dwd full & SCD") {
    val createSchemaSql =
      """
        |CREATE SCHEMA IF NOT EXISTS ods;
        |CREATE SCHEMA IF NOT EXISTS dwd;
        |create extension if not exists "uuid-ossp";""".stripMargin
    execute(createSchemaSql, "postgres", 5432)

    val createOdsTableSql =
      """
        |drop table if exists ods.t_order;
        |create table if not exists ods.t_order
        |(
        |    order_sn          varchar(128),
        |    product_code      varchar(128),
        |    product_name      varchar(128),
        |    product_version   varchar(128),
        |    product_status    varchar(128),
        |    user_code         varchar(128),
        |    user_name         varchar(128),
        |    user_age          int,
        |    user_address      varchar(128),
        |    product_count     int,
        |    price             decimal(10, 4),
        |    discount          decimal(10, 4),
        |    order_status      varchar(128),
        |    order_create_time timestamp,
        |    order_update_time timestamp,
        |    job_id            varchar(128)
        |) ;""".stripMargin
    execute(createOdsTableSql, "postgres", 5432)

    val initOdsSql =
      """
        |insert into ods.t_order (job_id, order_sn, product_code, product_name, product_version, product_status, user_code, user_name, user_age, user_address, product_count, price, discount, order_status, order_create_time, order_update_time) values
        | (1, 'AAA', 'p1', '华为', 'mate40', '上架', 'u1', '张三', 12, '迎宾街道', 12, 20, 0.3, 2, '2022-04-04 10:00:00', '2022-04-08 10:00:00') -- 正常更新
        |,(1, 'BBB', 'p1', '华为', 'mate40', '上架', 'u1', '张三', 12, '迎宾街道', 12, 30, 0.3, 1, '2022-04-04 11:00:00', '2022-04-08 09:00:00') -- 迟到时间，不做更新，该状态还是1，不更新
        |,(1, 'DDD', 'p1', '华为', 'mate40', '上架', 'u1', '张三', 12, '迎宾街道', 12, 200, 0.4, 1, '2022-04-04 12:00:00', '2022-04-08 12:00:00') -- 新增
        |,(2, 'AAA', 'p1', '华为', 'mate40', '上架', 'u1', '张三', 12, '迎宾街道', 12, 20, 0.3, 2, '2022-04-04 10:00:00', '2022-04-08 10:00:00') -- 正常更新
        |,(2, 'BBB', 'p1', '华为', 'mate40', '上架', 'u1', '张三', 12, '迎宾街道', 12, 30, 0.3, 1, '2022-04-04 10:00:00', '2022-04-08 09:00:00') -- 迟到时间，不做更新，该状态还是1，不更新
        |,(2, 'DDD', 'p1', '华为', 'mate40', '上架', 'u1', '张三', 12, '迎宾街道', 12, 200, 0.4, 2, '2022-04-04 10:00:00', '2022-04-09 10:00:00') -- 状态更新
        |,(2, 'EEE', 'p1', '华为', 'mate40', '上架', 'u1', '张三', 12, '迎宾街道', 12, 200, 1.4, 1, '2022-04-09 10:00:00', '2022-04-09 10:00:00'); -- 新增""".stripMargin
    execute(initOdsSql, "postgres", 5432)

    val createDwdTableSql =
      """
        |create extension if not exists "uuid-ossp";
        |drop table if exists dwd.t_fact_order;
        |create table dwd.t_fact_order(
        |    onedata_order_id	varchar(128) default uuid_generate_v1(),
        |    order_sn	varchar(128),
        |    product_id	varchar(128),
        |    user_id	varchar(128),
        |    product_count	int,
        |    price	decimal(10,4),
        |    discount	decimal(10,4),
        |    order_status	varchar(128),
        |    order_create_time	timestamp,
        |    order_update_time	timestamp,
        |    actual	decimal(10,4),
        |    start_time timestamp,
        |    end_time timestamp,
        |    is_active varchar(1),
        |    is_latest varchar(1)
        |);
        |
        |drop table if exists dwd.t_dim_product;
        |create table dwd.t_dim_product(
        |	   id  varchar(128) default uuid_generate_v1(), -- 渐变id
        |    mid varchar(128),
        |    name varchar(128),
        |    version varchar(128),
        |    status varchar(128),
        |    create_time timestamp,
        |    update_time timestamp,
        |    start_time timestamp,
        |    end_time timestamp,
        |    is_active varchar(1),
        |    is_latest varchar(1),
        |    is_auto_created varchar(1)
        |);
        |
        |drop table if exists dwd.t_dim_user;
        |create table dwd.t_dim_user(
        |	   user_id  varchar(128) default uuid_generate_v1(), -- 渐变id
        |    user_code varchar(128),
        |    user_name varchar(128),
        |    user_age int,
        |    user_address varchar(128),
        |    create_time timestamp,
        |    update_time timestamp,
        |    start_time timestamp,
        |    end_time timestamp,
        |    is_active varchar(1),
        |    is_latest varchar(1),
        |    is_auto_created varchar(1)
        |);""".stripMargin
    execute(createDwdTableSql, "postgres", 5432)

    val initDwdSql =
      """
        |insert into dwd.t_dim_product(id, mid, name, version, status, create_time, update_time, start_time, end_time, is_active, is_latest, is_auto_created) values
        | ('3abd0495-9abe-44a0-b95b-0e42aeadc807', 'p1', '华为', 'mate40', '上架', '2021-01-01 10:00:00', '2021-01-01 10:00:00', '2021-01-01 10:00:00', null, '1', '1', '0');
        |
        |insert into dwd.t_dim_user(user_id, user_code, user_name, user_age, user_address, create_time, update_time, start_time, end_time, is_active, is_latest, is_auto_created) values
        |('06347be1-f752-4228-8480-4528a2166e14', 'u1', '张三', 12, '胜利街道', '2020-01-01 10:00:00', '2020-01-01 10:00:00', '2020-01-01 10:00:00', null, '1', '1', '0');
        |
        |insert into dwd.t_fact_order(onedata_order_id, order_sn, product_id, user_id, product_count, price, discount, order_status, order_create_time, order_update_time, actual, start_time, end_time, is_active, is_latest) values
        |('3abd0495-9abe-44a0-b95b-0e42aeadc909', 'AAA', '3abd0495-9abe-44a0-b95b-0e42aeadc807', '06347be1-f752-4228-8480-4528a2166e14', 12, 20, 0.3, 1, '2022-04-04 10:00:00', '2022-04-04 10:00:00', 19.7, '2022-04-04 10:00:00', null, '1', '1'),
        |('3abd0495-9abe-44a0-b95b-0e42aeadc919', 'BBB', '3abd0495-9abe-44a0-b95b-0e42aeadc807', '06347be1-f752-4228-8480-4528a2166e14', 12, 10, 0.3, 2, '2022-04-04 11:00:00', '2022-04-08 10:00:00', 9.7, '2022-04-04 10:00:00', null, '1', '1'),
        |('3abd0495-9abe-44a0-b95b-0e42aeadc929', 'CCC', '3abd0495-9abe-44a0-b95b-0e42aeadc807', '06347be1-f752-4228-8480-4528a2166e14', 12, 30, 0.3, 2, '2022-04-04 12:00:00', '2022-04-07 10:00:00', 29.7, '2022-04-04 10:00:00', null, '1', '1');
        |""".stripMargin
    execute(initDwdSql, "postgres", 5432)

    val truncateOdsLogSql =
      """
        |truncate table job_log;""".stripMargin
    executeMigration(truncateOdsLogSql)

    val truncateOdsLogStepSql =
      """
        |truncate table step_log;""".stripMargin
    executeMigration(truncateOdsLogStepSql)

    val initOdsLogSql =
      """
        |insert into job_log (job_id, job_name, job_period, job_schedule_id, data_range_start, data_range_end, status) values
        |(1, 'source_to_ods', 1440, 'source_to_ods-20220408000000', '20220408000000', '20220409000000', 'SUCCESS'),
        |(2, 'source_to_ods', 1440, 'source_to_ods-20220408000000', '20220408000000', '20220409000000', 'SUCCESS');
        |""".stripMargin
    executeMigration(initOdsLogSql)

    runJob(Array("single-job",
      s"--name=ods_to_dwd_full_sc", "--period=1440",
      "--local", s"--property=$filePath"))

    var dwdDf = readTable("postgres", 5432, "dwd.t_fact_order")
    dwdDf = dwdDf.withColumn("end_time", when(col("order_sn") === "CCC" && col("is_latest") === "0", getTimeStampFromStr("2999-09-09 00:00:00")).otherwise(col("end_time")))
      .withColumn("start_time", when(col("order_sn") === "CCC" && col("is_latest") === "1", getTimeStampFromStr("2999-09-09 00:00:00")).otherwise(col("start_time")))
      .withColumn("order_update_time", when(col("order_sn") === "CCC" && col("is_latest") === "1", getTimeStampFromStr("2999-09-09 00:00:00")).otherwise(col("order_update_time")))
    dwdDf.show()

    val newCreatedData = Seq(
      Row("BBB", "3abd0495-9abe-44a0-b95b-0e42aeadc807", "06347be1-f752-4228-8480-4528a2166e14", 12, BigDecimal(10.0000), BigDecimal(0.3000), "2", getTimeStampFromStr("2022-04-04 11:00:00"), getTimeStampFromStr("2022-04-08 10:00:00"), BigDecimal(9.7), getTimeStampFromStr("2022-04-04 10:00:00"), null, "1", "1"),
      Row("CCC", "3abd0495-9abe-44a0-b95b-0e42aeadc807", "06347be1-f752-4228-8480-4528a2166e14", 12, BigDecimal(30.0000), BigDecimal(0.3000), "2", getTimeStampFromStr("2022-04-04 12:00:00"), getTimeStampFromStr("2022-04-07 10:00:00"), BigDecimal(29.7), getTimeStampFromStr("2022-04-04 10:00:00"), getTimeStampFromStr("2999-09-09 00:00:00"), "1", "0"),
      Row("AAA", "3abd0495-9abe-44a0-b95b-0e42aeadc807", "06347be1-f752-4228-8480-4528a2166e14", 12, BigDecimal(20.0000), BigDecimal(0.3000), "1", getTimeStampFromStr("2022-04-04 10:00:00"), getTimeStampFromStr("2022-04-04 10:00:00"), BigDecimal(19.7), getTimeStampFromStr("2022-04-04 10:00:00"), getTimeStampFromStr("2022-04-08 10:00:00"), "1", "0"),
      Row("CCC", "3abd0495-9abe-44a0-b95b-0e42aeadc807", "06347be1-f752-4228-8480-4528a2166e14", 12, BigDecimal(30.0000), BigDecimal(0.3000), "2", getTimeStampFromStr("2022-04-04 12:00:00"), getTimeStampFromStr("2999-09-09 00:00:00"), BigDecimal(29.7), getTimeStampFromStr("2999-09-09 00:00:00"), null, "0", "1"),
      Row("AAA", "3abd0495-9abe-44a0-b95b-0e42aeadc807", "06347be1-f752-4228-8480-4528a2166e14", 12, BigDecimal(20.0000), BigDecimal(0.3000), "2", getTimeStampFromStr("2022-04-04 10:00:00"), getTimeStampFromStr("2022-04-08 10:00:00"), BigDecimal(19.7), getTimeStampFromStr("2022-04-08 10:00:00"), null, "1", "1"),
      Row("DDD", "3abd0495-9abe-44a0-b95b-0e42aeadc807", "06347be1-f752-4228-8480-4528a2166e14", 12, BigDecimal(200.0000), BigDecimal(0.4000), "1", getTimeStampFromStr("2022-04-04 12:00:00"), getTimeStampFromStr("2022-04-08 12:00:00"), BigDecimal(199.6), getTimeStampFromStr("2022-04-04 12:00:00"), getTimeStampFromStr("2022-04-09 10:00:00"), "1", "0"),
      Row("DDD", "3abd0495-9abe-44a0-b95b-0e42aeadc807", "06347be1-f752-4228-8480-4528a2166e14", 12, BigDecimal(200.0000), BigDecimal(0.4000), "2", getTimeStampFromStr("2022-04-04 10:00:00"), getTimeStampFromStr("2022-04-09 10:00:00"), BigDecimal(199.6), getTimeStampFromStr("2022-04-09 10:00:00"), null, "1", "1"),
      Row("EEE", "3abd0495-9abe-44a0-b95b-0e42aeadc807", "06347be1-f752-4228-8480-4528a2166e14", 12, BigDecimal(200.0000), BigDecimal(1.4000), "1", getTimeStampFromStr("2022-04-09 10:00:00"), getTimeStampFromStr("2022-04-09 10:00:00"), BigDecimal(198.6), getTimeStampFromStr("2022-04-09 10:00:00"), null, "1", "1")
    )
    val dwdOrderShouldBe = spark.createDataFrame(
      spark.sparkContext.parallelize(newCreatedData),
      StructType(dwdOrderSCDSchema)
    )
    dwdOrderShouldBe.show()

    assertSmallDataFrameEquality(dwdDf.drop("job_id", "onedata_order_id"), dwdOrderShouldBe, orderedComparison = false)

  }
  it("ods -> dwd incremental & SCD") {
    val createSchemaSql =
      """
        |CREATE SCHEMA IF NOT EXISTS ods;
        |CREATE SCHEMA IF NOT EXISTS dwd;
        |create extension if not exists "uuid-ossp";""".stripMargin
    execute(createSchemaSql, "postgres", 5432)

    val createOdsTableSql =
      """
        |drop table if exists ods.t_order;
        |create table if not exists ods.t_order
        |(
        |    order_sn          varchar(128),
        |    product_code      varchar(128),
        |    product_name      varchar(128),
        |    product_version   varchar(128),
        |    product_status    varchar(128),
        |    user_code         varchar(128),
        |    user_name         varchar(128),
        |    user_age          int,
        |    user_address      varchar(128),
        |    product_count     int,
        |    price             decimal(10, 4),
        |    discount          decimal(10, 4),
        |    order_status      varchar(128),
        |    order_create_time timestamp,
        |    order_update_time timestamp,
        |    job_id            varchar(128)
        |) ;""".stripMargin
    execute(createOdsTableSql, "postgres", 5432)

    val initOdsSql =
      """
        |insert into ods.t_order (job_id, order_sn, product_code, product_name, product_version, product_status, user_code, user_name, user_age, user_address, product_count, price, discount, order_status, order_create_time, order_update_time) values
        | (1, 'AAA', 'p1', '华为', 'mate40', '上架', 'u1', '张三', 12, '迎宾街道', 12, 20, 0.3, 2, '2022-04-04 10:00:00', '2022-04-08 10:00:00') -- 正常更新
        |,(1, 'BBB', 'p1', '华为', 'mate40', '上架', 'u1', '张三', 12, '迎宾街道', 12, 30, 0.3, 1, '2022-04-04 11:00:00', '2022-04-08 09:00:00') -- 迟到时间，不做更新，该状态还是1，不更新
        |,(1, 'DDD', 'p1', '华为', 'mate40', '上架', 'u1', '张三', 12, '迎宾街道', 12, 200, 0.4, 1, '2022-04-04 12:00:00', '2022-04-08 12:00:00') -- 新增
        |,(2, 'DDD', 'p1', '华为', 'mate40', '上架', 'u1', '张三', 12, '迎宾街道', 12, 200, 0.4, 2, '2022-04-04 10:00:00', '2022-04-09 10:00:00') -- 状态更新
        |,(2, 'EEE', 'p1', '华为', 'mate40', '上架', 'u1', '张三', 12, '迎宾街道', 12, 200, 1.4, 1, '2022-04-09 10:00:00', '2022-04-09 10:00:00'); -- 新增""".stripMargin
    execute(initOdsSql, "postgres", 5432)

    val createDwdTableSql =
      """
        |create extension if not exists "uuid-ossp";
        |drop table if exists dwd.t_fact_order;
        |create table dwd.t_fact_order(
        |    onedata_order_id	varchar(128) default uuid_generate_v1(),
        |    order_sn	varchar(128),
        |    product_id	varchar(128),
        |    user_id	varchar(128),
        |    product_count	int,
        |    price	decimal(10,4),
        |    discount	decimal(10,4),
        |    order_status	varchar(128),
        |    order_create_time	timestamp,
        |    order_update_time	timestamp,
        |    actual	decimal(10,4),
        |    start_time timestamp,
        |    end_time timestamp,
        |    is_active varchar(1),
        |    is_latest varchar(1)
        |);
        |
        |drop table if exists dwd.t_dim_product;
        |create table dwd.t_dim_product(
        |	   id  varchar(128) default uuid_generate_v1(), -- 渐变id
        |    mid varchar(128),
        |    name varchar(128),
        |    version varchar(128),
        |    status varchar(128),
        |    create_time timestamp,
        |    update_time timestamp,
        |    start_time timestamp,
        |    end_time timestamp,
        |    is_active varchar(1),
        |    is_latest varchar(1),
        |    is_auto_created varchar(1)
        |);
        |
        |drop table if exists dwd.t_dim_user;
        |create table dwd.t_dim_user(
        |	   user_id  varchar(128) default uuid_generate_v1(), -- 渐变id
        |    user_code varchar(128),
        |    user_name varchar(128),
        |    user_age int,
        |    user_address varchar(128),
        |    create_time timestamp,
        |    update_time timestamp,
        |    start_time timestamp,
        |    end_time timestamp,
        |    is_active varchar(1),
        |    is_latest varchar(1),
        |    is_auto_created varchar(1)
        |);""".stripMargin
    execute(createDwdTableSql, "postgres", 5432)

    val initDwdSql =
      """
        |insert into dwd.t_dim_product(id, mid, name, version, status, create_time, update_time, start_time, end_time, is_active, is_latest, is_auto_created) values
        | ('3abd0495-9abe-44a0-b95b-0e42aeadc807', 'p1', '华为', 'mate40', '上架', '2021-01-01 10:00:00', '2021-01-01 10:00:00', '2021-01-01 10:00:00', null, '1', '1', '0');
        |
        |insert into dwd.t_dim_user(user_id, user_code, user_name, user_age, user_address, create_time, update_time, start_time, end_time, is_active, is_latest, is_auto_created) values
        |('06347be1-f752-4228-8480-4528a2166e14', 'u1', '张三', 12, '胜利街道', '2020-01-01 10:00:00', '2020-01-01 10:00:00', '2020-01-01 10:00:00', null, '1', '1', '0');
        |
        |insert into dwd.t_fact_order(onedata_order_id, order_sn, product_id, user_id, product_count, price, discount, order_status, order_create_time, order_update_time, actual, start_time, end_time, is_active, is_latest) values
        |('3abd0495-9abe-44a0-b95b-0e42aeadc909', 'AAA', '3abd0495-9abe-44a0-b95b-0e42aeadc807', '06347be1-f752-4228-8480-4528a2166e14', 12, 20, 0.3, 1, '2022-04-04 10:00:00', '2022-04-04 10:00:00', 19.7, '2022-04-04 10:00:00', null, '1', '1'),
        |('3abd0495-9abe-44a0-b95b-0e42aeadc919', 'BBB', '3abd0495-9abe-44a0-b95b-0e42aeadc807', '06347be1-f752-4228-8480-4528a2166e14', 12, 10, 0.3, 2, '2022-04-04 11:00:00', '2022-04-08 10:00:00', 9.7, '2022-04-04 10:00:00', null, '1', '1'),
        |('3abd0495-9abe-44a0-b95b-0e42aeadc929', 'CCC', '3abd0495-9abe-44a0-b95b-0e42aeadc807', '06347be1-f752-4228-8480-4528a2166e14', 12, 30, 0.3, 2, '2022-04-04 12:00:00', '2022-04-07 10:00:00', 29.7, '2022-04-04 10:00:00', null, '1', '1');
        |""".stripMargin
    execute(initDwdSql, "postgres", 5432)

    val truncateOdsLogSql =
      """
        |truncate table job_log;""".stripMargin
    executeMigration(truncateOdsLogSql)

    val truncateOdsLogStepSql =
      """
        |truncate table step_log;""".stripMargin
    executeMigration(truncateOdsLogStepSql)

    val initOdsLogSql =
      """
        |insert into job_log (job_id, job_name, job_period, job_schedule_id, data_range_start, data_range_end, status) values
        |(1, 'source_to_ods', 1440, 'source_to_ods-20220408000000', '20220408000000', '20220409000000', 'SUCCESS'),
        |(2, 'source_to_ods', 1440, 'source_to_ods-20220408000000', '20220408000000', '20220409000000', 'SUCCESS');
        |""".stripMargin
    executeMigration(initOdsLogSql)

    runJob(Array("single-job",
      s"--name=ods_to_dwd_incremental_sc", "--period=1440",
      "--local", s"--property=$filePath"))

    val dwdDf = readTable("postgres", 5432, "dwd.t_fact_order")
    dwdDf.show()

    val newCreatedData = Seq(
      Row("BBB", "3abd0495-9abe-44a0-b95b-0e42aeadc807", "06347be1-f752-4228-8480-4528a2166e14", 12, BigDecimal(10.0000), BigDecimal(0.3000), "2", getTimeStampFromStr("2022-04-04 11:00:00"), getTimeStampFromStr("2022-04-08 10:00:00"), BigDecimal(9.7), getTimeStampFromStr("2022-04-04 10:00:00"), null, "1", "1"),
      Row("CCC", "3abd0495-9abe-44a0-b95b-0e42aeadc807", "06347be1-f752-4228-8480-4528a2166e14", 12, BigDecimal(30.0000), BigDecimal(0.3000), "2", getTimeStampFromStr("2022-04-04 12:00:00"), getTimeStampFromStr("2022-04-07 10:00:00"), BigDecimal(29.7), getTimeStampFromStr("2022-04-04 10:00:00"), null, "1", "1"),
      Row("AAA", "3abd0495-9abe-44a0-b95b-0e42aeadc807", "06347be1-f752-4228-8480-4528a2166e14", 12, BigDecimal(20.0000), BigDecimal(0.3000), "1", getTimeStampFromStr("2022-04-04 10:00:00"), getTimeStampFromStr("2022-04-04 10:00:00"), BigDecimal(19.7), getTimeStampFromStr("2022-04-04 10:00:00"), getTimeStampFromStr("2022-04-08 10:00:00"), "1", "0"),
      Row("AAA", "3abd0495-9abe-44a0-b95b-0e42aeadc807", "06347be1-f752-4228-8480-4528a2166e14", 12, BigDecimal(20.0000), BigDecimal(0.3000), "2", getTimeStampFromStr("2022-04-04 10:00:00"), getTimeStampFromStr("2022-04-08 10:00:00"), BigDecimal(19.7), getTimeStampFromStr("2022-04-08 10:00:00"), null, "1", "1"),
      Row("DDD", "3abd0495-9abe-44a0-b95b-0e42aeadc807", "06347be1-f752-4228-8480-4528a2166e14", 12, BigDecimal(200.0000), BigDecimal(0.4000), "1", getTimeStampFromStr("2022-04-04 12:00:00"), getTimeStampFromStr("2022-04-08 12:00:00"), BigDecimal(199.6), getTimeStampFromStr("2022-04-04 12:00:00"), getTimeStampFromStr("2022-04-09 10:00:00"), "1", "0"),
      Row("DDD", "3abd0495-9abe-44a0-b95b-0e42aeadc807", "06347be1-f752-4228-8480-4528a2166e14", 12, BigDecimal(200.0000), BigDecimal(0.4000), "2", getTimeStampFromStr("2022-04-04 10:00:00"), getTimeStampFromStr("2022-04-09 10:00:00"), BigDecimal(199.6), getTimeStampFromStr("2022-04-09 10:00:00"), null, "1", "1"),
      Row("EEE", "3abd0495-9abe-44a0-b95b-0e42aeadc807", "06347be1-f752-4228-8480-4528a2166e14", 12, BigDecimal(200.0000), BigDecimal(1.4000), "1", getTimeStampFromStr("2022-04-09 10:00:00"), getTimeStampFromStr("2022-04-09 10:00:00"), BigDecimal(198.6), getTimeStampFromStr("2022-04-09 10:00:00"), null, "1", "1")
    )
    val dwdOrderShouldBe = spark.createDataFrame(
      spark.sparkContext.parallelize(newCreatedData),
      StructType(dwdOrderSCDSchema)
    )
    dwdOrderShouldBe.show()

    assertSmallDataFrameEquality(dwdDf.drop("job_id", "onedata_order_id"), dwdOrderShouldBe, orderedComparison = false)

  }
}
