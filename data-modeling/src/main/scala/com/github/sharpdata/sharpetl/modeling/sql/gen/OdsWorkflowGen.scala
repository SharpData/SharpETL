package com.github.sharpdata.sharpetl.modeling.sql.gen

import com.github.sharpdata.sharpetl.modeling.excel.model.OdsTable.{OdsModeling, OdsModelingColumn}
import com.google.common.base.Strings.isNullOrEmpty
import com.github.sharpdata.sharpetl.core.datasource.config._
import com.github.sharpdata.sharpetl.core.syntax.{Workflow, WorkflowStep}
import com.github.sharpdata.sharpetl.core.util.Constants.DataSourceType.HIVE
import com.github.sharpdata.sharpetl.core.util.Constants.IncrementalType._
import com.github.sharpdata.sharpetl.core.util.Constants.LoadType._
import com.github.sharpdata.sharpetl.core.util.Constants.WriteMode
import com.github.sharpdata.sharpetl.core.util.ETLConfig.partitionColumn
import com.github.sharpdata.sharpetl.modeling.sql.dialect.SqlDialect.{getSqlDialect, quote}

object OdsWorkflowGen {

  def genWorkflow(odsModeling: OdsModeling, workflowName: String): Workflow = {
    val step = new WorkflowStep()
    step.step = "1"
    step.source = getDataSourceConfig(odsModeling)
    step.target = getTargetSourceConfig(odsModeling)


    val dataSourceType = odsModeling.odsTableConfig.sourceType
    val additionalCols = List(s"$${JOB_ID} AS ${quote("job_id", dataSourceType)}")
    val columns = buildColumnString(odsModeling, additionalCols)
    val sourceDb = quote(odsModeling.odsTableConfig.sourceDb, dataSourceType)
    val sourceTable = quote(odsModeling.odsTableConfig.sourceTable, dataSourceType)
    val steps = odsModeling.odsTableConfig.updateType match {
      case INCREMENTAL =>
        step.writeMode = if (dataSourceType == HIVE) WriteMode.OVER_WRITE else WriteMode.APPEND
        val filterColumnName = quote(incrColumn(odsModeling), dataSourceType)
        val partitionClause = genPartitionClause(odsModeling)

        step.sqlTemplate =
          s"""|SELECT $columns$partitionClause
              |FROM $sourceDb.$sourceTable
              |WHERE $filterColumnName >= '$${DATA_RANGE_START}' AND $filterColumnName < '$${DATA_RANGE_END}'
              |""".stripMargin
        List(step)
      case FULL | DIFF =>
        step.writeMode = WriteMode.OVER_WRITE
        step.sqlTemplate =
          s"""|SELECT $columns,\n '$${DATA_RANGE_START}' AS $partitionColumn
              |FROM $sourceDb.$sourceTable
              |""".stripMargin
        List(step)
      case AUTO_INC_ID =>
        step.target = new VariableDataSourceConfig()
        val idColumn = odsModeling.columns.filter(_.primaryKeyColumn).head.sourceColumn
        step.sqlTemplate =
          s"""
             |SELECT $${DATA_RANGE_START} AS ${quote("lowerBound", dataSourceType)},
             |       MAX(${quote(idColumn, dataSourceType)}) AS ${quote("upperBound", dataSourceType)}
             |FROM $sourceDb.$sourceTable
             |""".stripMargin

        val stepRead = new WorkflowStep()
        stepRead.step = "2"
        stepRead.source = getDataSourceConfig(odsModeling)
        stepRead.source.asInstanceOf[DBDataSourceConfig].numPartitions = "4"
        stepRead.source.asInstanceOf[DBDataSourceConfig].lowerBound = "${lowerBound}"
        stepRead.source.asInstanceOf[DBDataSourceConfig].upperBound = "${upperBound}"
        stepRead.source.asInstanceOf[DBDataSourceConfig].partitionColumn = "idColumn"
        stepRead.target = getTargetSourceConfig(odsModeling)
        stepRead.writeMode = WriteMode.UPSERT //in case of partial failure
        stepRead.sqlTemplate =
          s"""|SELECT $columns
              |FROM $sourceDb.$sourceTable
              |WHERE ${quote(idColumn, dataSourceType)} > $${lowerBound}
              |  AND ${quote(idColumn, dataSourceType)} <= $${upperBound}
              |""".stripMargin
        List(step, stepRead)
    }
    // scalastyle:off
    Workflow(workflowName, odsModeling.odsTableConfig.period, odsModeling.odsTableConfig.updateType,
      "timewindow", //TODO: update later
      null, null, null, 0, null, false, null, Map(), steps
    )
    // scalastyle:on
  }


  private def getTargetSourceConfig(odsModeling: OdsModeling): DBDataSourceConfig = {
    val targetSourceConfig = new DBDataSourceConfig()
    targetSourceConfig.dataSourceType = odsModeling.odsTableConfig.targetType
    targetSourceConfig.dbName = odsModeling.odsTableConfig.targetDb
    targetSourceConfig.tableName = odsModeling.odsTableConfig.targetTable
    targetSourceConfig
  }

  private def getDataSourceConfig(odsModeling: OdsModeling): DBDataSourceConfig = {
    val config = new DBDataSourceConfig()
    config.tableName = odsModeling.odsTableConfig.sourceTable
    config.dbName = odsModeling.odsTableConfig.sourceDb
    config.dataSourceType = odsModeling.odsTableConfig.sourceType
    config
  }

  private def buildColumnString(odsModeling: OdsModeling, additionalCols: List[String]): String = {
    (odsModeling.columns
      .map(col => getColumnAsString(col, odsModeling.odsTableConfig.sourceType)) ++ additionalCols)
      .mkString(",\n       ")
  }

  private def getColumnAsString(col: OdsModelingColumn, sourceType: String): String = {
    val exprOrSourceCol = if (isNullOrEmpty(col.extraColumnExpression)) quote(col.sourceColumn, sourceType) else col.extraColumnExpression
    s"""$exprOrSourceCol AS ${quote(col.targetColumn, sourceType)}"""
  }

  private def incrColumn(odsModeling: OdsModeling): String = {
    assert(odsModeling.columns.count(c => c.incrementalColumn) == 1)
    odsModeling.columns.filter(c => c.incrementalColumn).map(_.sourceColumn).head
  }


  def genPartitionClause(odsModeling: OdsModeling): String = {
    val partitionFormat = if (isNullOrEmpty(odsModeling.odsTableConfig.partitionFormat)) "" else odsModeling.odsTableConfig.partitionFormat
    val dialect = getSqlDialect(odsModeling.odsTableConfig.sourceType)
    val partitionClause = partitionFormat match {
      case "" => ""
      case "year/month/day" =>
        val timeFormat = odsModeling.odsTableConfig.timeFormat
        val partitionField = odsModeling.columns.filter(_.incrementalColumn).head.sourceColumn
        List(
          s"${dialect.year(partitionField, timeFormat)} as ${dialect.quoteIdentifier("year")}",
          s"${dialect.month(partitionField, timeFormat)} as ${dialect.quoteIdentifier("month")}",
          s"${dialect.day(partitionField, timeFormat)} as ${dialect.quoteIdentifier("day")}"
        ).mkString(",\n       ")
      case _ => ???
    }
    if (isNullOrEmpty(partitionClause)) {
      ""
    } else {
      s""",\n       $partitionClause""".stripMargin
    }
  }
}
