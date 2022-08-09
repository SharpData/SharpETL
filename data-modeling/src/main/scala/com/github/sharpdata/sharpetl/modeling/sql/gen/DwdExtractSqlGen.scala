package com.github.sharpdata.sharpetl.modeling.sql.gen

import com.github.sharpdata.sharpetl.modeling.excel.model.{DwdModeling, DwdModelingColumn}
import com.github.sharpdata.sharpetl.modeling.sql.dialect.SqlDialect
import com.github.sharpdata.sharpetl.core.datasource.config.DBDataSourceConfig
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.core.util.Constants.Separator.ENTER
import com.github.sharpdata.sharpetl.core.util.Constants.{IncrementalType, DataSourceType, WriteMode}
import com.github.sharpdata.sharpetl.core.util.ETLConfig.jobIdColumn
import com.github.sharpdata.sharpetl.core.util.StringUtil.{getTempName, isNullOrEmpty}
import SqlDialect.quote

import scala.collection.mutable.ArrayBuffer

object DwdExtractSqlGen {
  val ZIP_ID_FLAG = "zip_id_flag"

  def genExtractStep(dwdModding: DwdModeling, stepIndex: Int): List[WorkflowStep] = {
    val sourceType: String = dwdModding.dwdTableConfig.sourceType
    val rowFilterExpression = dwdModding.dwdTableConfig.rowFilterExpression
    val steps = ArrayBuffer[WorkflowStep]()

    val step = new WorkflowStep

    step.setStep(stepIndex.toString)

    val sourceConfig = new DBDataSourceConfig
    sourceConfig.setDataSourceType(dwdModding.dwdTableConfig.sourceType)
    sourceConfig.setDbName(dwdModding.dwdTableConfig.sourceDb)
    sourceConfig.setTableName(dwdModding.dwdTableConfig.sourceTable)
    step.setSourceConfig(sourceConfig)

    val targetConfig = new DBDataSourceConfig
    targetConfig.setDataSourceType(DataSourceType.TEMP)
    targetConfig.setTableName(getTempName(sourceConfig.tableName, "extracted"))
    step.setTargetConfig(targetConfig)

    step.setWriteMode(WriteMode.OVER_WRITE)

    val rowFilterConfig = getRowFilterAsString(rowFilterExpression, sourceType)
    //  TODO 抽象接口，不同引擎有不同的实现。
    val selectColumn = dwdModding.columns
      .map {
        column => {
          if (ZIP_ID_FLAG.equals(column.extraColumnExpression)) {
            if (isNullOrEmpty(column.joinTable)) {
              s"\t${SqlDialect.surrogateKey(dwdModding.dwdTableConfig.sourceType)} as ${quote(getTargetColumn(column), sourceType)}"
            } else {
              ""
            }
          } else if (isNullOrEmpty(column.extraColumnExpression)) {
            s"\t${quote(getSourceColumn(column), sourceType)} as ${quote(getTargetColumn(column), sourceType)}"
          } else {
            if (isNullOrEmpty(getTargetColumn(column))) {
              "" // extract from fact col to dim col, so we didn't need select it here, we could use the expr later
            } else {
              s"\t${column.extraColumnExpression} as ${quote(getTargetColumn(column), sourceType)}"
            }
          }
        }
      }
      .filterNot(isNullOrEmpty)
      .mkString(s",$ENTER")

    val partitionCols = dwdModding.columns.filter(_.partitionColumn).map(_.sourceColumn)

    val whereClause = if (partitionCols.isEmpty) {
      s"${quote(jobIdColumn, sourceType)} = '$${DATA_RANGE_START}'"
    } else {
      // for year,month,day,hour,minute
      partitionCols.map(col => s"${quote(col, sourceType)} = '$${${col.toUpperCase}}'")
        .mkString("\n  and ")
    }

    val selectSql =
      s"""
         |select
         |$selectColumn
         |from ${quote(dwdModding.dwdTableConfig.sourceDb, sourceType)}.${quote(dwdModding.dwdTableConfig.sourceTable, sourceType)}
         |where $whereClause
         |$rowFilterConfig
         |""".stripMargin
    step.setSqlTemplate(selectSql)


    steps.append(step)
    steps.toList
  }

  def getSourceColumn(column: DwdModelingColumn): String = {
    if (isNullOrEmpty(column.sourceColumn)) "" else column.sourceColumn
  }

  def getTargetColumn(column: DwdModelingColumn): String = {
    if (isNullOrEmpty(column.targetColumn)) getSourceColumn(column) else column.targetColumn
  }

  def getRowFilterAsString(rowFilterExpression: String, sourceType: String): String = {
    if( rowFilterExpression == null) {
      ""
    } else {
      rowFilterExpression.split(",").map(it => {
        val rowFilterExpressionArray = it.split("=")
        val colum: String =rowFilterExpressionArray(0)
        val columnValue = rowFilterExpressionArray(1)
        s"""AND ${quote(colum, sourceType)} = '$columnValue'"""
      }).mkString(" ")
    }}

}
