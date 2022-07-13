package com.github.sharpdata.sharpetl.spark.transformation

import com.github.sharpdata.sharpetl.core.util.ETLConfig.jobIdColumn
import com.github.sharpdata.sharpetl.core.util.ETLLogger
import com.github.sharpdata.sharpetl.spark.utils.ETLSparkSession.sparkSession
import SCDTransformer.{getAppendSelectClause, getPartitionClause}
import com.github.sharpdata.sharpetl.spark.datasource.HiveDataSource
import com.github.sharpdata.sharpetl.spark.utils.SparkCatalogUtil
import org.apache.spark.sql._

object NonSCDTransformer extends Transformer {

  // scalastyle:off
  def genSql(args: Map[String, String]): String = {
    val odsViewName = args("odsViewName")
    val dwViewName = args("dwViewName")

    val partitionField = args.getOrElse("partitionField", "")
    val partitionByClause = args("primaryFields")
      .split(",")
      .mkString(", ")
    val createTimeField = args("createTimeField")
    val updateTimeField = args("updateTimeField")
    val surrogateField = args("surrogateField")
    val dropUpdateTimeField = args.getOrElse("dropUpdateTimeField", false.toString).toBoolean
    val orderByClause = updateTimeField

    val partitionFormat = args.getOrElse("partitionFormat", "")

    val timeFormat = s"'${args.getOrElse("timeFormat", "yyyy-MM-dd HH:mm:ss")}'"

    val updateCols = updateTimeField.split(",").toSet
    val createCols = createTimeField.split(",").toSet
    val excludeCols =
      Set(jobIdColumn, "start_time", "end_time", "is_latest", "is_active",
        "rank_num", "count_num", "dense_rank_num", "data_status", "match_value",
        "max_dense_rank_num", "older_data_end_time") ++ partitionFormat.split("/").toSet

    val cols = SparkCatalogUtil.getAllColNamesOfTempTable(odsViewName)
    val selectClause = cols.filterNot(excludeCols.contains).mkString(",")

    val droppedSortColumns = if (dropUpdateTimeField) {
      cols.filterNot(excludeCols.contains).filterNot(updateCols.contains).filterNot(createCols.contains).mkString(",")
    } else {
      selectClause
    }

    s"""
       |select $droppedSortColumns,
       |       ${getPartitionClause(partitionField, partitionFormat, timeFormat)}
       |       ${getAppendSelectClause(args)}
       |             from (select $selectClause,
       |                          row_number() over (partition by $partitionByClause order by $orderByClause desc) as rank_num
       |                   from (
       |                      select $selectClause from $odsViewName
       |                      union all
       |                      select $selectClause from $dwViewName
       |                   )
       |             )
       |             where rank_num = 1
       |""".stripMargin
  }
  // scalastyle:on

  override def transform(args: Map[String, String]): DataFrame = {
    val sql = genSql(args)
    ETLLogger.info(s"[Sql]:$sql")
    new HiveDataSource().load(sparkSession, sql)
  }
}
