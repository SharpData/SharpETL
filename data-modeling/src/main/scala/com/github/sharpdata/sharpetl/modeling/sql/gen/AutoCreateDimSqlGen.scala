package com.github.sharpdata.sharpetl.modeling.sql.gen

import com.github.sharpdata.sharpetl.modeling.excel.model.{CreateDimMode, DimTable, DwdModelingColumn}
import com.google.common.base.Strings.isNullOrEmpty
import com.github.sharpdata.sharpetl.core.datasource.config.DBDataSourceConfig
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.core.util.Constants.DataSourceType.SPARK_SQL
import com.github.sharpdata.sharpetl.core.util.Constants.{IncrementalType, DataSourceType, WriteMode}
import com.github.sharpdata.sharpetl.core.util.StringUtil.getTempName
import com.github.sharpdata.sharpetl.modeling.sql.dialect.SqlDialect.quote

// scalastyle:off
object AutoCreateDimSqlGen {
  val DISTINCT_COUNT_NUM = "distinct_count_num"

  def parseDimensionSql(sourceTableName: String,
                        dimTable: DimTable): String = {
    lazy val joinOnCols = dimTable.joinOnColumns.map(column => s"`${column.joinTempFieldPrefix}${joinColOrSourceCol(column)}`")
    lazy val updateTimeCols = dimTable.updateTimeCols.map(column => s"`${joinColOrSourceCol(column)}`")
    lazy val partitionByClause = s",\n       row_number() OVER (PARTITION BY ${joinOnCols.mkString(",")} ORDER BY ${updateTimeCols.mkString(",")} DESC) as row_number"

    val autoCreateColumnsSelectExpr = dimTable
      .autoCreateColumns
      .map(column => s"uuid() as `${column.joinTableColumn}`")
    val otherColumnsSelectExpr = dimTable.noneAutoCreateDimIdColumns
      .map(column =>
        s"`${column.joinTempFieldPrefix}${joinColOrSourceCol(column)}` as ${quote(joinColOrSourceCol(column), SPARK_SQL)}")
    val additionalColsSelectExpr = dimTable.additionalCols
      .map(column => s"`${column.sourceColumn}`")
    val selectClause = (autoCreateColumnsSelectExpr ++ (otherColumnsSelectExpr :+ s"'1' as ${quote("is_auto_created", SPARK_SQL)}") ++ additionalColsSelectExpr).mkString(",\n       ")
    val partitionedSelectionExpr =
      (((dimTable.cols ++ dimTable.updateTimeCols ++ dimTable.createTimeCols)
        .map(col => quote(joinColOrSourceCol(col), SPARK_SQL)) :+ quote("is_auto_created", SPARK_SQL))
        ++ dimTable.partitionCols.map(col => quote(joinColOrSourceCol(col), SPARK_SQL))).mkString(",")

    val whereNotNullClause = dimTable
      .joinOnColumns
      .map(column => s"`${column.joinTempFieldPrefix}${joinColOrSourceCol(column)}` is not null").mkString("\n       and")

    val once = dimTable.cols.exists(_.createDimMode == CreateDimMode.ONCE)

    if (once) {
      s"""
         |select $partitionedSelectionExpr from (
         |  select $selectClause$partitionByClause
         |  from $sourceTableName $sourceTableName
         |  where ($whereNotNullClause)
         |    and (`auto_created_${dimTable.dimTable}_status` = 'new')
         |) where row_number = 1
         |""".stripMargin
    } else {
      s"""
         |select $selectClause
         |from $sourceTableName $sourceTableName
         |where ($whereNotNullClause)
         |  and (
         |      `auto_created_${dimTable.dimTable}_status` = 'new'
         |       or `auto_created_${dimTable.dimTable}_status` = 'updated')
         |""".stripMargin
    }
  }

  def tmpStepToTempStep(steps: List[WorkflowStep], index: Int, sourceTempTable: String, func: String, sql: String): List[WorkflowStep] = {
    val step = new WorkflowStep
    step.setStep(index.toString)

    val sourceConfig = new DBDataSourceConfig
    sourceConfig.setDataSourceType(DataSourceType.TEMP)
    sourceConfig.setTableName(sourceTempTable)
    step.setSourceConfig(sourceConfig)

    val targetConfig = new DBDataSourceConfig
    targetConfig.setDataSourceType(DataSourceType.TEMP)
    targetConfig.setTableName(getTempName(sourceTempTable, func))
    step.setTargetConfig(targetConfig)
    step.setWriteMode(WriteMode.OVER_WRITE)
    step.setSqlTemplate(sql)

    steps :+ step
  }

  def parsePartitionClause(dimTables: Seq[DimTable], sourceTableName: String): String = {
    val cols = dimTables.flatMap(_.additionalCols).map(col => s"`$sourceTableName`.`${col.sourceColumn}` as `${col.sourceColumn}`").distinct
    if (cols.isEmpty) {
      ""
    } else {
      s",\n       ${cols.mkString(",\n       ")}"
    }
  }

  def genGroupedDimensionSql(dimTables: Seq[DimTable], tempTableName: String, sourceTableName: String): String = {
    val joinFactTempSelectClause = parseJoinFactTempSelectClause(dimTables)
    val joinFactTempJoinClause = parseJoinFactTempJoinClause(dimTables)
    val joinStatus = parseJoinStatusClause(dimTables)
    val partitionCols = parsePartitionClause(dimTables, sourceTableName)

    s"""
       |select $joinFactTempSelectClause,
       |       $joinStatus$partitionCols
       |from $tempTableName `$sourceTableName`
       |     $joinFactTempJoinClause
       |""".stripMargin
  }

  def parseJoinFactTempSelectClause(dimTables: Seq[DimTable]): String =
    dimTables
      .flatMap(_.noneAutoCreateDimIdColumns)
      .sortBy(_.joinTable)
      .map(column =>
        s"${sourceColOrExpr(column)} as `${column.joinTempFieldPrefix}${joinColOrSourceCol(column)}`"
      )
      .mkString(",\n       ")

  def parseJoinFactTempJoinClause(dimTables: Seq[DimTable]): String =
    dimTables
      .sortBy(_.dimTable)
      .map(dimTable => {
        val joinOnClause = dimTable
          .joinOnColumns
          .map(column => s"${sourceColOrExpr(column)} = `${dimTable.dimTable}`.`${joinColOrSourceCol(column)}`")
          .mkString("\n                   and ")
        s"""|     left join `${dimTable.cols.head.joinDb}`.`${dimTable.dimTable}` `${dimTable.dimTable}` -- TODO: year/month/day
            |               on $joinOnClause
            |""".stripMargin
      })
      .mkString("")
      .trim

  def parseJoinStatusClause(dimTables: Seq[DimTable]): String = {
    def caseClause(dimTable: DimTable): String = {
      val newClause = s"""${dimTable.joinOnColumns.map(col => s"`${dimTable.dimTable}`.`${col.joinOn}` is null").mkString(" or ")}"""

      val updatedClause = dimTable
        .noneAutoCreateDimIdColumns
        .map(column =>
          s"${sourceColOrExpr(column)} != `${dimTable.dimTable}`.`${joinColOrSourceCol(column)}`"
        )
        .mkString(" or\n                ")

      s"""case
         |           when (
         |                $newClause
         |           ) then 'new'
         |           when (
         |                $updatedClause
         |           ) then 'updated'
         |           else 'nochange'
         |       end as `auto_created_${dimTable.dimTable}_status`""".stripMargin
    }


    dimTables
      .sortBy(_.dimTable)
      .map(caseClause)
      .mkString(",\n       ")
  }

  def joinColOrSourceCol(column: DwdModelingColumn): String = {
    if (isNullOrEmpty(column.joinTableColumn)) column.sourceColumn else column.joinTableColumn
  }

  def targetColOrSourceCol(column: DwdModelingColumn): String = {
    if (isNullOrEmpty(column.targetColumn)) column.sourceColumn else column.targetColumn
  }

  def sourceColOrExpr(column: DwdModelingColumn): String = {
    if (isNullOrEmpty(column.sourceColumn)) {
      if (column.extraColumnExpression.contains(" ")) {
        column.extraColumnExpression
      } else {
        s"`${column.sourceTable}`.`${column.extraColumnExpression}`"
      }
    } else {
      s"`${column.sourceTable}`.`${column.sourceColumn}`"
    }
  }
}
