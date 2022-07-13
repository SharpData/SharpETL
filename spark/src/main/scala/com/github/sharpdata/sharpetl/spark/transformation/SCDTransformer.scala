package com.github.sharpdata.sharpetl.spark.transformation

import com.github.sharpdata.sharpetl.spark.datasource.HiveDataSource
import com.github.sharpdata.sharpetl.core.util.Constants.LoadType.{FULL, INCREMENTAL}
import com.github.sharpdata.sharpetl.core.util.ETLConfig.jobIdColumn
import com.github.sharpdata.sharpetl.core.util.{DateUtil, ETLLogger}
import com.github.sharpdata.sharpetl.spark.utils.ETLSparkSession.sparkSession
import com.github.sharpdata.sharpetl.spark.utils.SparkCatalogUtil
import org.apache.spark.sql._

import scala.util.Try

object SCDTransformer extends Transformer {
  def getAppendSelectClause(args: Map[String, String]): String = {
    s"""       '${args("jobId")}' as $jobIdColumn""".stripMargin.trim
  }

  def getCaseClause(args: Map[String, String],
                    hardDeleteEndTime: String): String = {
    args.get("dwUpdateType") match {
      case Some(INCREMENTAL) =>
        s"""                                 -- incremental --
           |                                 -- unchanged active data
           |                                 when is_active = 1 and is_latest = 1 and count_num = 1 and data_status = 'older_or_unchanged'
           |                                     then array('1', '1', end_time)""".stripMargin
      case Some(FULL) =>
        s"""                                 -- full --
           |                                 -- deleted: new deleted data(full hard delete)
           |                                 when is_active = 1 and is_latest = 1 and count_num = 1 and data_status = 'older_or_unchanged'
           |                                     then array('0', '1', '$hardDeleteEndTime')
           |                                 -- unchanged deleted data(deleted before)
           |                                 when is_active = 0 and is_latest = 1 and count_num = 1 and data_status = 'older_or_unchanged'
           |                                     then array('0', '1', end_time)""".stripMargin
      case _ => ???
    }
  }

  // scalastyle:off
  def scdSql(args: Map[String, String]): String = {
    val odsViewName = args("odsViewName")
    val dwViewName = args("dwViewName")
    val partitionByClause = args("primaryFields")
      .split(",")
      .mkString(", ")

    val partitionField = args.getOrElse("partitionField", "")
    val createTimeField = args("createTimeField")
    val updateTimeField = args("updateTimeField")
    val surrogateField = args("surrogateField")
    val dropUpdateTimeField = args.getOrElse("dropUpdateTimeField", false.toString).toBoolean
    val orderByClause = updateTimeField

    val partitionFormat = args.getOrElse("partitionFormat", "")

    val timeFormat = s"'${args.getOrElse("timeFormat", "yyyy-MM-dd HH:mm:ss")}'"

    val partitionClause = getPartitionClause(partitionField, partitionFormat, timeFormat)

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

    val distinctByClause = s"(${cols.filterNot(excludeCols.contains).filterNot(it => it == surrogateField).mkString(",")})"

    val time = Try(DateUtil.YYYY_MM_DD_HH_MM_SS.parse(args("dataRangeStart")).getTime)
      .getOrElse(args("dataRangeStart").toLong)

    val hardDeleteEndTime = DateUtil
      .YYYY_MM_DD_HH_MM_SS
      .format(time - 1 * 1000)

    val appendSelectClause = getAppendSelectClause(args)
    val caseClause = getCaseClause(args, hardDeleteEndTime)

    val joinClause = args("primaryFields").split(",").map(col => s"updated.$col = dw.$col").mkString(" and ")
    val doesNotExistsInDwClause = args("primaryFields").split(",").map(col => s"dw.$col is null").mkString(" or ")

    s"""
       |with ods as (select $selectClause
       |             from (select $selectClause,
       |                          row_number() over (partition by $distinctByClause order by $orderByClause desc) as rank_num
       |                   from $odsViewName)
       |             where rank_num = 1), -- distinct ods data by all cols
       |     dw as (select $droppedSortColumns,
       |                   start_time,
       |                   end_time,
       |                   is_active,
       |                   is_latest
       |            from $dwViewName), -- affected dw data
       |     dw_history as (select *
       |                    from dw
       |                    where is_latest = 0), -- passively update dw history data, no changes
       |     ods_and_latest_dw as (select ${droppedSortColumns.split(",").map(col => s"updated.$col").mkString(",")},
       |                                  case
       |                                      -- first time create, using min(createTime, updateTime) as start_time
       |                                      when updated.pre_update_time is null and ($doesNotExistsInDwClause) then updated.$createTimeField
       |                                      else updated.$updateTimeField
       |                                  end as start_time,
       |                                  updated.end_time,
       |                                  case
       |                                      when (updated.rank_num = 1) then '1'
       |                                      else '0'
       |                                  end as `is_active`,
       |                                  case
       |                                      when (updated.rank_num = 1) then '1'
       |                                      else '0'
       |                                  end as `is_latest`,
       |                                  case
       |                                      when (updated.rank_num = 1) then 'latest'
       |                                      else 'newer'
       |                                  end as `data_status`
       |                           from (select $selectClause,
       |                                          row_number() over rank_window as rank_num,
       |                                          lag($updateTimeField) over rank_window as end_time,
       |                                          lead($updateTimeField) over rank_window as pre_update_time
       |                                 from ods
       |                                       window
       |                                          rank_window as (partition by $partitionByClause order by $orderByClause desc)
       |                                 ) updated
       |                           left join dw on $joinClause and dw.is_active = '1' and dw.is_latest = '1'
       |                           union all
       |                           select *,
       |                                  'older_or_unchanged' as data_status
       |                           from dw
       |                           where is_latest = 1), -- latest & newer ods data union all older & unchanged dw data
       |     count_and_rank as (select *,
       |                               count(1) over dense_rank_window as count_num,
       |                               dense_rank() over dense_rank_window as dense_rank_num
       |                        from ods_and_latest_dw
       |                            window
       |                                dense_rank_window as (partition by $partitionByClause order by start_time)),
       |     max_rank_and_end_time as (select *,
       |                                      first_value(dense_rank_num) over part_window as max_dense_rank_num,
       |                                      lag(start_time) over part_window as older_data_end_time
       |                               from count_and_rank
       |                                  window
       |                                      part_window as (partition by $partitionByClause order by start_time desc)),
       |     match_result as (select $droppedSortColumns,
       |                             start_time,
       |                             case
       |                                 -- new or updated data
       |                                 when data_status = 'latest' or data_status = 'newer'
       |                                     then array(is_active, is_latest, end_time)
       |                                 -- updated: older version of updated data
       |                                 when data_status = 'older_or_unchanged' and is_active = 1 and is_latest = 1 and max_dense_rank_num > 1
       |                                     then array('0', '0', older_data_end_time)
       |                                 -- re-add: re-add deleted data
       |                                 when data_status = 'older_or_unchanged' and is_active = 0 and is_latest = 1 and count_num = 2
       |                                     then array('0', '0', end_time)
       |                                 -- full or re-run(incremental or full)
       |                                 -- unchanged: duplicated active unchanged data
       |                                 when data_status = 'older_or_unchanged' and is_active = 1 and is_latest = 1 and count_num = 2 and
       |                                      max_dense_rank_num = 1
       |                                     then array('1', '1', end_time)
       |$caseClause
       |                             end as match_value
       |                      from max_rank_and_end_time
       |                      where not (
       |                          -- filter --
       |                          -- full or rerun(incremental or full)
       |                          -- unchanged: duplicated active unchanged data
       |                                  is_active = 1
       |                              and is_latest = 1
       |                              and count_num = 2
       |                              and data_status = 'latest'
       |                              and max_dense_rank_num = 1
       |                          )
       |     )
       |select $droppedSortColumns,
       |       cast(start_time as timestamp) as start_time,
       |       cast(match_value[2] as timestamp) as end_time,
       |       match_value[0] as is_active,
       |       match_value[1] as is_latest,
       |       $partitionClause
       |       $appendSelectClause
       |from match_result
       |union all
       |-- incremental or full --
       |-- history not latest unchangedDataHistory
       |select $droppedSortColumns,
       |       cast(start_time as timestamp) as start_time,
       |       cast(end_time as timestamp) as end_time,
       |       is_active,
       |       is_latest,
       |       $partitionClause
       |       $appendSelectClause
       |from dw_history
       |""".stripMargin
  }
  // scalastyle:on

  def getPartitionClause(partitionField: String, partitionFormat: String, timeFormat: String): String = {
    partitionFormat match {
      case "" => ""
      case "year/month/day" =>
        s"""from_unixtime(unix_timestamp($partitionField, $timeFormat), 'yyyy') as year,
           |       from_unixtime(unix_timestamp($partitionField, $timeFormat), 'MM') as month,
           |       from_unixtime(unix_timestamp($partitionField, $timeFormat), 'dd') as day,""".stripMargin
      case _ => ???
    }
  }

  override def transform(args: Map[String, String]): DataFrame = {
    val sql = scdSql(args)
    ETLLogger.info(s"[SCD Sql]:$sql")
    new HiveDataSource().load(sparkSession, sql)
  }
}
