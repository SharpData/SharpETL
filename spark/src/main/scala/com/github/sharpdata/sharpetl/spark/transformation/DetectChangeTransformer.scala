package com.github.sharpdata.sharpetl.spark.transformation

import com.github.sharpdata.sharpetl.spark.utils.{ETLSparkSession, SparkCatalogUtil}
import org.apache.spark.sql.DataFrame


// $COVERAGE-OFF$

/**
 * 自动检测变动
 */
object DetectChangeTransformer extends Transformer {

  /**
   * 比较hive里ads table得到变动的数据view
   * @param args 参数
   *          sourceViewName - 新数据源view
   *          targetHiveTableName  - 目标HiveTable名,用于读取字段列表
   *          pkCols      - ADS主键列表,用逗号分隔, 例如:data,copm_code,material_code
   *          ignoreCols  - 不需要比较差别的字段列表, 逗号分隔, 样例同上
   * @return 差异的表table view, 主要看is_newest字段
   *         is_newest = 0 代表此数据在新view里不存在, 需要在ads里删除
   *         is_newest = 1 代表有变动的, 需要在ads里upsert
   */
  override def transform(args: Map[String, String]): DataFrame = {
    val sourceViewName = args("sourceViewName").toString // 新数据源
    val targetHiveTableName = args("targetHiveTableName").toString // 目标hive table name
    val targetDbName = args("targetDbName").toString // 目标 db name
    val pkCols = args("pkCols").toString.split(",") // pg的pk有哪些字段
    val ignoreCols = args("ignoreCols").toString.split(",") // 不做查重的字段名列表

    val spark = ETLSparkSession.getHiveSparkSession()
    // 得到目标hive表的所有字段名
    val hiveCols = SparkCatalogUtil.getAllColNames(targetDbName, targetHiveTableName)

    // PK拼成的str
    val pkColsStr = pkCols.map(str => s"`$str`").mkString(",")
    // 非PK非不比较字段的str
    val normalColsStr = hiveCols
      .filter(str => !pkCols.contains(str) && !ignoreCols.contains(str))
      .map(str => s"`$str`").mkString(",")
    // 所有字段str
    val allColStr = hiveCols.map(str => s"`$str`").mkString(",")

    val allSql =
      s"""
         |SELECT
         |  $allColStr
         |  ,0 AS is_newest
         |FROM
         |  $targetHiveTableName
         |UNION ALL
         |SELECT
         |  $allColStr
         |  ,1 AS is_newest
         |FROM
         |  $sourceViewName
         |""".stripMargin

    val sql =
      s"""
         |WITH
         |  all_in_one AS (
         |  $allSql
         |)
         |, window_table_1 AS (
         |  SELECT *,
         |    COUNT(1) OVER (PARTITION BY $pkColsStr ) AS count_num,
         |    dense_rank() OVER (
         |      partition by $pkColsStr
         |      order by
         |        $normalColsStr
         |    )  as dense_rank_num
         |  FROM all_in_one
         |)
         |, window_table_2 as (
         |    SELECT *,
         |      first_value(dense_rank_num) over (
         |        partition by
         |          $pkColsStr
         |        order by dense_rank_num desc
         |      ) as max_dense_rank_num
         |   from window_table_1
         |)
         |SELECT *
         |FROM window_table_2
         |WHERE
         |    (max_dense_rank_num = 2 and is_newest = 1)
         |    OR
         |    count_num = 1
         |""".stripMargin

    spark.sql(sql)

  }
}
// $COVERAGE-ON$
