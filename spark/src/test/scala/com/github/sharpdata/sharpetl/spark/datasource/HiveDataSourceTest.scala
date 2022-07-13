package com.github.sharpdata.sharpetl.spark.datasource

import com.github.sharpdata.sharpetl.spark.job.SparkSessionTestWrapper
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.core.util.Constants.WriteMode.MERGE_WRITE
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should


class HiveDataSourceTest extends AnyFlatSpec with MockitoSugar with ArgumentMatchersSugar with SparkSessionTestWrapper with should.Matchers {

  it should "extract partition selection" in {
    val schema = List(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("year", StringType, true),
      StructField("month", StringType, true),
      StructField("day", StringType, true)
    )

    val data = Seq(
      Row(1, "111", "2021", "12", "01"),
      Row(2, "222", "2021", "12", "01"),
      Row(1, null, "2021", "12", "02"),
      Row(2, "222", "2021", "11", "01")
    )

    val testDf = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      StructType(schema)
    )

    val sourceTableName = "test_hive_union_sql"
    testDf.createOrReplaceTempView(sourceTableName)

    val step = new WorkflowStep

    step.sql =
      """
        |select `hour` as `hour`,
        |       `minute` as `minute`,
        |       `year` as   `year`,
        |       `month` as   `month`,
        |       `day` as   `day`
        |from `${{developer}}`.`${{pre_ods_card_result}}`
        |where  `year`= '2022' and `month`= '12' and `day`= '12'
        |       and `hour`= '10' and `minute`= '10';
        |""".stripMargin

    val sql: String = new HiveDataSource().selfUnionClause(sourceTableName, "hive_table", Array("year", "month", "day"), MERGE_WRITE, step)
    sql should be(
      """union all
        |select * from hive_table where ((year = 2021 and month = 12 and day = 01) or (year = 2021 and month = 12 and day = 02) or (year = 2021 and month = 11 and day = 01)) and !(`year`= '2022' and `month`= '12' and `day`= '12'
        |       and `hour`= '10' and `minute`= '10')""".stripMargin)
  }

  it should "extract partition selection for complex sql" in {
    val schema = List(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("year", StringType, true),
      StructField("month", StringType, true),
      StructField("day", StringType, true),
      StructField("hour", StringType, true),
      StructField("minute", StringType, true)
    )

    val data = Seq(
      Row(1, "111", "2021", "12", "01", "10", "10"),
      Row(2, "222", "2021", "12", "01", "10", "10"),
      Row(1, null, "2021", "12", "02", "10", "10"),
      Row(2, "222", "2021", "11", "01", "10", "10")
    )

    val testDf = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      StructType(schema)
    )

    val sourceTableName = "test_mergewrite"
    testDf.createOrReplaceTempView(sourceTableName)

    val step = new WorkflowStep

    step.sql =
      s"""
        |with window_result as (
        |  SELECT t.*,
        |      row_number() over(partition by t.`id`, t.`name` order by t.`id` desc) as `insert_id`
        |  FROM $sourceTableName t
        |  where  `year`= '2022' and `month`= '12' and `day`= '12'
        |               and `hour`= '10' and `minute`= '10'),
        |  distinct_result as (
        |  SELECT `id` as `id`,
        |       `name` as `name`,
        |       `hour` as `hour`,
        |       `minute` as `minute`,
        |       `year` as   `year`,
        |       `month` as   `month`,
        |       `day` as   `day`
        |  FROM window_result WHERE `insert_id` = 1)
        |select `id` as `id`,
        |       `name` as `name`,
        |       `hour` as `hour`,
        |       `minute` as `minute`,
        |       `year` as   `year`,
        |       `month` as   `month`,
        |       `day` as   `day`
        |from distinct_result
        |where `year`= '2022' and `month`= '12' and `day`= '12'
        |              and `hour`= '10' and `minute`= '10'
        |""".stripMargin

    val sql: String = new HiveDataSource().selfUnionClause(sourceTableName, "hive_table", Array("year", "month", "day"), MERGE_WRITE, step)
    sql should be(
      """union all
        |select * from hive_table where ((year = 2021 and month = 12 and day = 01) or (year = 2021 and month = 12 and day = 02) or (year = 2021 and month = 11 and day = 01)) and !(`year`= '2022' and `month`= '12' and `day`= '12'
        |              and `hour`= '10' and `minute`= '10')""".stripMargin)
  }
}
