package com.github.sharpdata.sharpetl.modeling.sql.gen

import com.github.sharpdata.sharpetl.modeling.excel.model.DwdModeling
import com.github.sharpdata.sharpetl.core.datasource.config.DBDataSourceConfig
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.core.util.Constants.DataSourceType.HIVE
import com.github.sharpdata.sharpetl.core.util.Constants.Separator.ENTER
import com.github.sharpdata.sharpetl.core.util.Constants.{IncrementalType, DataSourceType, WriteMode}
import com.github.sharpdata.sharpetl.core.util.StringUtil.{getTempName, isNullOrEmpty}
import com.github.sharpdata.sharpetl.modeling.sql.dialect.SqlDialect.quote
import AutoCreateDimSqlGen.joinColOrSourceCol
import DwdExtractSqlGen.{ZIP_ID_FLAG, getTargetColumn}

object DwdTransformSqlGen2 {
  def generateReadMatchTableStep(steps: List[WorkflowStep], dwdModding: DwdModeling, stepIndex: Int): (List[WorkflowStep], Int) = {
    var index = stepIndex - 1 // we will add it back in the loop
    val stepsTemp: List[WorkflowStep] = dwdModding.columns
      //      TODO check joinDbType、joinDb和joinTable必须同时有值或没值
      .filter(column => !isNullOrEmpty(column.joinTable))
      .groupBy(column => (column.joinDbType, column.joinDb, column.joinTable))
      .toList
      .sortBy(_._1._3)
      .map {
        case ((joinDbType, joinDb, joinTable), dimColumns) => {
          val step = new WorkflowStep
          index = index + 1
          step.setStep(index.toString)

          val sourceConfig = new DBDataSourceConfig
          sourceConfig.setDataSourceType(dimColumns.head.joinDbType)
          sourceConfig.setDbName(joinDb)
          sourceConfig.setTableName(joinTable)
          step.setSourceConfig(sourceConfig)

          val targetConfig = new DBDataSourceConfig
          targetConfig.setDataSourceType(DataSourceType.TEMP)
          targetConfig.setTableName(getTempName(joinDb + "_" + joinTable, "matched"))
          step.setTargetConfig(targetConfig)
          step.setWriteMode(WriteMode.APPEND)
          val selectColumn = dimColumns
            .map {
              column => {
                if (ZIP_ID_FLAG.equals(column.extraColumnExpression)) {
                  quote(column.joinTableColumn, joinDbType)
                } else if (!isNullOrEmpty(column.joinOn)) {
                  quote(column.joinOn, joinDbType)
                } else {
                  ""
                }
              }
            }
            .filterNot(isNullOrEmpty)
            .mkString(", ")

          val selectSql =
            s"""
               |select
               | $selectColumn, ${quote("start_time", joinDbType)}, ${quote("end_time", joinDbType)}
               |from ${quote(joinDb, joinDbType)}.${quote(joinTable, joinDbType)}
               |""".stripMargin
          step.setSqlTemplate(selectSql)

          step
        }
      }
      .toList
    (steps ++ stepsTemp, index + 1)
  }

  def genMatchStep(steps: List[WorkflowStep], dwdModding: DwdModeling, stepIndex: Int): (List[WorkflowStep], Int) = {
    val extractTableName = steps.head.target.asInstanceOf[DBDataSourceConfig].tableName
    val extractTableNameDialect = quote(extractTableName, HIVE)
    val businessTimeColumnName = getTargetColumn(dwdModding.columns.filter(column => column.businessCreateTime).head)

    val surrogateKeys = dwdModding.columns.filter(_.extraColumnExpression == ZIP_ID_FLAG).map(_.targetColumn).mkString("|")

    val index = stepIndex
    val sqlParts = dwdModding.columns
      .filterNot(column => isNullOrEmpty(column.joinTable))
      .groupBy(column => (column.joinDbType, column.joinDb, column.joinTable))
      .toList
      .sortBy(_._1._3)
      .map {
        case ((_, joinDb, joinTable), dimColumns) => {
          val joinTableName = quote(getTempName(joinDb + "_" + joinTable, "matched"), HIVE)

          val selectColumn = dimColumns
            .filter(column => ZIP_ID_FLAG.equals(column.extraColumnExpression))
            .head
          val selectColumnName = quote(selectColumn.joinTableColumn, HIVE)
          val selectTargetColumnName = quote(selectColumn.targetColumn, HIVE)
          val selectStr =
            s"""case when $joinTableName.$selectColumnName is null then '-1'
               |\t\telse $joinTableName.$selectColumnName end as $selectTargetColumnName""".stripMargin

          val joinOnColumn = dimColumns
            .filter(column => !isNullOrEmpty(column.joinOn))
            .head
          val joinOnStr =
            s"""left join $joinTableName
               | on $extractTableNameDialect.${quote(getTargetColumn(joinOnColumn), HIVE)} = $joinTableName.${quote(joinOnColumn.joinOn, HIVE)}
               | and $extractTableNameDialect.${quote(businessTimeColumnName, HIVE)} >= $joinTableName.${quote("start_time", HIVE)}
               | and ($extractTableNameDialect.${quote(businessTimeColumnName, HIVE)} < $joinTableName.${quote("end_time", HIVE)}
               |      or $joinTableName.${quote("end_time", HIVE)} is null)
               |""".stripMargin
          (selectStr, joinOnStr)
        }
      }
    if (sqlParts.isEmpty) {
      (steps, index)
    } else {
      val step = new WorkflowStep
      step.setStep(index.toString)

      val sourceConfig = new DBDataSourceConfig
      sourceConfig.setDataSourceType(DataSourceType.TEMP)
      sourceConfig.setTableName(extractTableName)
      step.setSourceConfig(sourceConfig)

      val targetConfig = new DBDataSourceConfig
      targetConfig.setDataSourceType(DataSourceType.TEMP)
      targetConfig.setTableName(getTempName(sourceConfig.tableName, "joined"))
      step.setTargetConfig(targetConfig)
      step.setWriteMode(WriteMode.APPEND)
      val selectSql =
        s"""select
           |\t$extractTableNameDialect.*,
           |\t${sqlParts.map(o => o._1).mkString(",\n\t")}
           |from $extractTableNameDialect
           |${sqlParts.map(o => o._2).mkString(ENTER)}
           |""".stripMargin
      step.setSqlTemplate(selectSql)

      (steps :+ step, index + 1)
    }
  }


  def genMatchStepInHive(steps: List[WorkflowStep], dwdModding: DwdModeling, stepIndex: Int): (List[WorkflowStep], Int) = {
    val extractTableName = steps.head.target.asInstanceOf[DBDataSourceConfig].tableName
    val extractTableNameDialect = quote(extractTableName, HIVE)
    val businessTimeColumnName = getTargetColumn(dwdModding.columns.filter(column => column.businessCreateTime).head)

    val index = stepIndex
    val sqlParts = dwdModding.columns
      .filterNot(column => isNullOrEmpty(column.joinTable))
      .groupBy(column => (column.joinDbType, column.joinDb, column.joinTable))
      .toList
      .sortBy(_._1._3)
      .map {
        case ((_, joinDb, joinTable), dimColumns) => {
          val joinTableName = quote(joinTable, HIVE)

          val selectColumn = dimColumns
            .filter(column => ZIP_ID_FLAG.equals(column.extraColumnExpression))
            .head
          val selectColumnName = quote(selectColumn.targetColumn, HIVE)
          val selectStr =
            s"""case when $joinTableName.${quote(joinColOrSourceCol(selectColumn), HIVE)} is null then '-1'
               |\t\telse $joinTableName.${quote(joinColOrSourceCol(selectColumn), HIVE)}
               |\tend as $selectColumnName""".stripMargin

          val joinOnColumn = dimColumns
            .filter(column => !isNullOrEmpty(column.joinOn))
            .head
          val joinOnStr =
            s"""left join ${quote(joinDb, HIVE)}.$joinTableName $joinTableName
               | on $extractTableNameDialect.${quote(getTargetColumn(joinOnColumn), HIVE)} = $joinTableName.${quote(joinOnColumn.joinOn, HIVE)}
               | and $extractTableNameDialect.${quote(businessTimeColumnName, HIVE)} >= $joinTableName.${quote("start_time", HIVE)}
               | and ($extractTableNameDialect.${quote(businessTimeColumnName, HIVE)} < $joinTableName.${quote("end_time", HIVE)}
               |      or $joinTableName.${quote("end_time", HIVE)} is null)
               |""".stripMargin
          (selectStr, joinOnStr)
        }
      }
    if (sqlParts.isEmpty) {
      (steps, index)
    } else {
      val step = new WorkflowStep
      step.setStep(index.toString)

      val sourceConfig = new DBDataSourceConfig
      sourceConfig.setDataSourceType(DataSourceType.TEMP)
      sourceConfig.setTableName(extractTableName)
      step.setSourceConfig(sourceConfig)

      val targetConfig = new DBDataSourceConfig
      targetConfig.setDataSourceType(DataSourceType.TEMP)
      targetConfig.setTableName(getTempName(sourceConfig.tableName, "joined"))
      step.setTargetConfig(targetConfig)
      step.setWriteMode(WriteMode.APPEND)
      val selectSql =
        s"""select
           |\t$extractTableNameDialect.*,
           |\t${sqlParts.map(o => o._1).mkString(",\n\t")}
           |from $extractTableNameDialect
           |${sqlParts.map(o => o._2).mkString(ENTER)}
           |""".stripMargin
      step.setSqlTemplate(selectSql)

      (steps :+ step, index + 1)
    }
  }
}
