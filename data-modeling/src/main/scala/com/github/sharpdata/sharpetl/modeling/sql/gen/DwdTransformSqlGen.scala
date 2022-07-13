package com.github.sharpdata.sharpetl.modeling.sql.gen

import com.github.sharpdata.sharpetl.modeling.excel.model.DwdModeling
import com.github.sharpdata.sharpetl.core.datasource.config.DBDataSourceConfig
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.core.util.Constants.DataSourceType.{HIVE, SPARK_SQL}
import com.github.sharpdata.sharpetl.core.util.Constants.Separator.ENTER
import com.github.sharpdata.sharpetl.core.util.Constants.{IncrementalType, DataSourceType, WriteMode}
import com.github.sharpdata.sharpetl.core.util.StringUtil.{getTempName, isNullOrEmpty}
import com.github.sharpdata.sharpetl.modeling.sql.dialect.SqlDialect.quote
import DwdExtractSqlGen.{ZIP_ID_FLAG, getTargetColumn}

import scala.collection.mutable

object DwdTransformSqlGen {
  def genTargetSelectStep(steps: List[WorkflowStep], dwdModding: DwdModeling, stepIndex: Int): List[WorkflowStep] = {
    val sourceType: String = SPARK_SQL
    val step = new WorkflowStep

    step.setStep(stepIndex.toString)

    val sourceConfig = new DBDataSourceConfig
    sourceConfig.setDataSourceType(DataSourceType.TEMP)
    val sourceTable = steps.last.target.asInstanceOf[DBDataSourceConfig].tableName
    sourceConfig.setTableName(sourceTable)

    if (dwdModding.columns.filterNot(column => isNullOrEmpty(column.qualityCheckRules)).size != 0) {
      val options = mutable.Map[String, String]()
      val logicPrimaryColumn = dwdModding.columns.filter(_.logicPrimaryColumn).map(_.targetColumn).mkString(",")
      options.put("idColumn", logicPrimaryColumn)
      dwdModding.columns.filterNot(column => isNullOrEmpty(column.qualityCheckRules)) foreach (column => {
        options.put(s"column.${getTargetColumn(column)}.qualityCheckRules", column.qualityCheckRules)
      })
      sourceConfig.setOptions(options.toMap)
    }
    step.setSourceConfig(sourceConfig)

    val targetConfig = new DBDataSourceConfig
    targetConfig.setDataSourceType(DataSourceType.TEMP)
    targetConfig.setTableName(getTempName(sourceTable, "target_selected"))
    step.setTargetConfig(targetConfig)

    step.setWriteMode(WriteMode.OVER_WRITE)

    val autoCreatedClause = if (dwdModding.dwdTableConfig.factOrDim.toLowerCase == "dim") {
      s"\t'0' as ${quote("is_auto_created", sourceType)}"
    } else {
      ""
    }

    var selectColumn = ""
    if (dwdModding.dwdTableConfig.targetType == HIVE) {
      selectColumn = (dwdModding.columns
        .filterNot(it => isNullOrEmpty(it.targetColumn))
        .map {
          column => {
            // no need to use `as` because all columns already the target col name
            s"\t${quote(getTargetColumn(column), sourceType)}"
          }
        } :+ autoCreatedClause)
        .filterNot(isNullOrEmpty)
        .mkString(s",$ENTER")
    } else {
      selectColumn = (dwdModding.columns
        .filterNot(it => isNullOrEmpty(it.targetColumn))
        .filterNot(it => isNullOrEmpty(it.joinTable) && ZIP_ID_FLAG.equals(it.extraColumnExpression))
        .map {
          column => {
            // no need to use `as` because all columns already the target col name
            s"\t${quote(getTargetColumn(column), sourceType)}"
          }
        } :+ autoCreatedClause)
        .filterNot(isNullOrEmpty)
        .mkString(s",$ENTER")
    }

    val selectSql =
      s"""
         |select
         |$selectColumn
         |from ${quote(sourceTable, sourceType)}
         |""".stripMargin

    step.setSqlTemplate(selectSql)

    steps :+ step
  }
}
