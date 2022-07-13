package com.github.sharpdata.sharpetl.spark.transformation

import com.github.sharpdata.sharpetl.spark.datasource.HiveDataSource
import com.github.sharpdata.sharpetl.core.util.Constants.LoadType.{FULL, INCREMENTAL}
import com.github.sharpdata.sharpetl.core.util.ETLConfig.{jobIdColumn, jobTimeColumn, partitionColumn}
import com.github.sharpdata.sharpetl.core.util.{DateUtil, ETLLogger}
import com.github.sharpdata.sharpetl.spark.utils.ETLSparkSession.sparkSession
import org.apache.spark.sql._

import java.sql.Timestamp
import java.time.Instant

object ZipTableTransformer extends Transformer {
  def getAppendSelectClause(args: collection.Map[String, String]): String = {
    val dateEnd = DateUtil.formatDate(
      args("dataRangeEnd"),
      DateUtil.YYYY_MM_DD_HH_MM_SS,
      DateUtil.YYYY_MM_DD_HH_MM_SS
    )
    s"""       '${args("dataRangeStart")}' as idempotent_key,
       |       '$dateEnd' as dw_insert_date,
       |       '${args("jobId")}' as $jobIdColumn,
       |       '${Timestamp.from(Instant.now())}' as $jobTimeColumn
       |       """.stripMargin.trim
  }

  def getCaseClause(args: collection.Map[String, String],
                    effectiveEndTime: String): String = {
    args.get("dwDataLoadType") match {
      case Some(INCREMENTAL) =>
        s"""                                 -- incremental --
           |                                 -- unchanged unchangedDataActiveIncremental
           |                                 when is_active = 1 and is_latest = 1 and count_num = 1 and is_newest = 0
           |                                     then array('1', '1', effective_end_time)""".stripMargin
      case Some(FULL) | None =>
        s"""                                 -- full --
           |                                 -- deleted deletedData
           |                                 when is_active = 1 and is_latest = 1 and count_num = 1 and is_newest = 0
           |                                     then array('0', '1', '$effectiveEndTime')
           |                                 -- unchanged deleted unchangedDataDeleted
           |                                 when is_active = 0 and is_latest = 1 and count_num = 1 and is_newest = 0
           |                                     then array('0', '1', effective_end_time)""".stripMargin
    }
  }

  // scalastyle:off
  def zipSql(args: collection.Map[String, String]): String = {
    val odsViewName = args("odsViewName")
    val dwViewName = args("dwViewName")
    val partitionByClause = args("primaryFields")
      .asInstanceOf[String]
      .split(",")
      .mkString(", ")
    val orderByClause = args("sortFields")
      .asInstanceOf[String]
      .split(",")
      .mkString(", ")

    val selectClause = s"""`($jobIdColumn|$jobTimeColumn|effective_start_time|effective_end_time|is_latest|is_active|idempotent_key|dw_insert_date|rank_num|count_num|dense_rank_num|is_newest|match_value|max_dense_rank_num|$partitionColumn)?+.+`"""

    val effectiveStartTime = DateUtil.formatDate(
      args("dataRangeStart"),
      DateUtil.YYYY_MM_DD_HH_MM_SS,
      DateUtil.YYYY_MM_DD_HH_MM_SS
    )
    val effectiveEndTime = DateUtil
      .YYYY_MM_DD_HH_MM_SS
      .format(DateUtil.YYYY_MM_DD_HH_MM_SS.parse(args("dataRangeStart")).getTime - 1 * 1000)
    val maxEffectiveEndTime = "9999-01-01 00:00:00"

    val appendSelectClause = getAppendSelectClause(args)
    val caseClause = getCaseClause(args, effectiveEndTime)

    s"""
       |with ods as (select $selectClause
       |             from (select $selectClause,
       |                          row_number() over (partition by $partitionByClause order by $orderByClause desc) as rank_num
       |                   from $odsViewName)
       |             where 1 = 1
       |               and rank_num = 1),
       |     dw as (select $selectClause,
       |                   effective_start_time,
       |                   effective_end_time,
       |                   is_active,
       |                   is_latest
       |            from $dwViewName),
       |     dw_history as (select *
       |                    from dw
       |                    where 1 = 1
       |                      and is_latest = 0),
       |     ods_and_latest_dw as (select $selectClause,
       |                                  '$effectiveStartTime' as effective_start_time,
       |                                  '$maxEffectiveEndTime' as effective_end_time,
       |                                  '1' as is_active,
       |                                  '1' as is_latest,
       |                                  '1' as is_newest
       |                           from ods
       |                           union all
       |                           select *,
       |                                  '0' as is_newest
       |                           from dw
       |                           where is_latest = 1),
       |     window_table_1 as (select *,
       |                               count(1) over w1 as count_num,
       |                               dense_rank() over w1 as dense_rank_num
       |                        from ods_and_latest_dw
       |                            window
       |                                w1 as (partition by $partitionByClause order by $orderByClause)),
       |     window_table_2 as (SELECT *,
       |                               first_value(dense_rank_num) over (
       |                                   partition by $partitionByClause
       |                                   order by $orderByClause desc
       |                                   ) as max_dense_rank_num
       |                        from window_table_1),
       |     match_result as (select $selectClause,
       |                             effective_start_time,
       |                             (case
       |                                 -- added newAddedData
       |                                 when count_num = 1 and is_newest = 1
       |                                     then array('1', '1', effective_end_time)
       |                                 -- updated new updatedDataNew
       |                                 when max_dense_rank_num = 2 and is_newest = 1
       |                                     then array('1', '1', effective_end_time)
       |                                 -- updated old updatedDataActive
       |                                 when is_active = 1 and is_latest = 1 and max_dense_rank_num = 2 and is_newest = 0
       |                                     then array('0', '0', '$effectiveEndTime')
       |                                 -- re added deleted updatedDataDeleted
       |                                 when is_active = 0 and is_latest = 1 and count_num = 2 and is_newest = 0
       |                                     then array('0', '0', effective_end_time)
       |                                 -- full or rerun(incremental or full)
       |                                 -- unchanged unchangedDataActiveDuplicate
       |                                 when is_active = 1 and is_latest = 1 and count_num = 2 and is_newest = 0 and
       |                                      max_dense_rank_num = 1
       |                                     then array('1', '1', effective_end_time)
       |$caseClause
       |                                 end) as match_value
       |                      from window_table_2
       |                      where 1 = 1
       |                        and not (
       |                          -- filter --
       |                          -- full or rerun(incremental or full)
       |                          -- unchanged unchangedDataActiveDuplicate
       |                                  is_active = 1
       |                              and is_latest = 1
       |                              and count_num = 2
       |                              and is_newest = 1
       |                              and max_dense_rank_num = 1
       |                          )
       |     )
       |select $selectClause,
       |       effective_start_time,
       |       match_value[2] as effective_end_time,
       |       match_value[0] as is_active,
       |       match_value[1] as is_latest,
       |       $appendSelectClause
       |from match_result
       |union all
       |-- incremental or full --
       |-- history not latest unchangedDataHistory
       |select $selectClause,
       |       effective_start_time,
       |       effective_end_time,
       |       is_active,
       |       is_latest,
       |       $appendSelectClause
       |from dw_history
       |""".stripMargin
  }
  // scalastyle:on

  def zip(args: Map[String, String]): DataFrame = {
    val sql = zipSql(args)
    ETLLogger.info(s"[Zip Table Sql]:$sql")
    new HiveDataSource().load(sparkSession, sql)
  }

  override def transform(args: Map[String, String]): DataFrame = {
    zip(args)
  }
}
