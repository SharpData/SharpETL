package com.github.sharpdata.sharpetl.modeling.sql.gen

import com.github.sharpdata.sharpetl.modeling.excel.model.{DimTable, DwdModeling}
import com.github.sharpdata.sharpetl.core.datasource.config.{DBDataSourceConfig, TransformationDataSourceConfig}
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.core.util.Constants.DataSourceType.SPARK_SQL
import com.github.sharpdata.sharpetl.core.util.Constants.{IncrementalType, LoadType, DataSourceType, WriteMode}
import com.github.sharpdata.sharpetl.core.util.StringUtil.{canNotBeEmpty, getTempName, isNullOrEmpty}
import com.github.sharpdata.sharpetl.modeling.sql.dialect.SqlDialect.quote
import AutoCreateDimSqlGen.targetColOrSourceCol
import DwdExtractSqlGen.ZIP_ID_FLAG

import scala.collection.mutable

object ScdSqlGen {
  def genScdStep(steps: List[WorkflowStep], dwdModding: DwdModeling, stepIndex: Int, isSCD: Boolean = true): List[WorkflowStep] = {
    val dwView = steps.last.target.asInstanceOf[DBDataSourceConfig].tableName

    val sourceType: String = dwdModding.dwdTableConfig.sourceType

    val step = new WorkflowStep

    step.setStep(stepIndex.toString)

    val sourceConfig = new TransformationDataSourceConfig
    sourceConfig.setDataSourceType(DataSourceType.TRANSFORMATION)
    sourceConfig.className = s"com.github.sharpdata.sharpetl.spark.transformation.${if (isSCD) "SCDTransformer" else "NonSCDTransformer"}"
    sourceConfig.methodName = "transform"
    sourceConfig.args = Map(
      canNotBeEmpty("odsViewName", s"${dwdModding.dwdTableConfig.sourceTable}__target_selected"),
      canNotBeEmpty("dwViewName", dwView),
      canNotBeEmpty("primaryFields", dwdModding.columns.filter(_.logicPrimaryColumn).map(_.targetColumn).mkString(",")),
      canNotBeEmpty("partitionField", dwdModding.columns.filter(_.businessCreateTime).map(_.targetColumn).mkString(",")),
      canNotBeEmpty("partitionFormat", dwdModding.columns.filter(_.partitionColumn).map(_.targetColumn).mkString("/")),
      canNotBeEmpty("updateTimeField", dwdModding.columns.filter(_.businessUpdateTime).map(_.targetColumn).head),
      canNotBeEmpty("createTimeField", dwdModding.columns.filter(_.businessCreateTime).map(_.targetColumn).head),
      canNotBeEmpty("dwUpdateType", if (dwdModding.dwdTableConfig.updateType == "full") LoadType.FULL else LoadType.INCREMENTAL),
      canNotBeEmpty("timeFormat", "yyyy-MM-dd HH:mm:ss"),
      ("surrogateField", getSurrogateField(dwdModding, dwdModding.dwdTableConfig.targetTable))
    )
    step.setSourceConfig(sourceConfig)

    val targetConfig = new DBDataSourceConfig
    targetConfig.setDataSourceType(sourceType)
    targetConfig.setDbName(dwdModding.dwdTableConfig.targetDb)
    targetConfig.setTableName(dwdModding.dwdTableConfig.targetTable)
    step.setTargetConfig(targetConfig)

    step.writeMode = WriteMode.OVER_WRITE


    steps :+ step
  }

  def genDwdViewStep(steps: List[WorkflowStep], dwdModding: DwdModeling, stepIndex: Int): List[WorkflowStep] = {
    val sourceType: String = SPARK_SQL

    val step = new WorkflowStep

    step.setStep(stepIndex.toString)

    val sourceConfig = new DBDataSourceConfig
    sourceConfig.setDataSourceType(dwdModding.dwdTableConfig.targetType)
    sourceConfig.setDbName(dwdModding.dwdTableConfig.targetDb)
    sourceConfig.setTableName(dwdModding.dwdTableConfig.targetTable)
    step.setSourceConfig(sourceConfig)

    val targetConfig = new DBDataSourceConfig
    targetConfig.setDataSourceType(DataSourceType.TEMP)
    targetConfig.setTableName(getTempName(sourceConfig.tableName, "changed_partition_view"))
    step.setTargetConfig(targetConfig)

    val selectSql =
      s"""
         |select *
         |from ${quote(sourceConfig.dbName, sourceType)}.${quote(sourceConfig.tableName, sourceType)}
         |$${DWD_UPDATED_PARTITION}
         |""".stripMargin

    step.setSqlTemplate(selectSql)


    steps :+ step
  }


  // incremental & scd
  def genDwdPartitionClauseStep(steps: List[WorkflowStep], dwdModding: DwdModeling, stepIndex: Int, isSCD: Boolean = true): List[WorkflowStep] = {
    val sourceType: String = SPARK_SQL

    val step = new WorkflowStep

    step.setStep(stepIndex.toString)

    val sourceConfig = new DBDataSourceConfig
    sourceConfig.setDataSourceType(dwdModding.dwdTableConfig.targetType)
    sourceConfig.setDbName(dwdModding.dwdTableConfig.targetDb)
    sourceConfig.setTableName(dwdModding.dwdTableConfig.targetTable)
    step.setSourceConfig(sourceConfig)

    val targetConfig = new DBDataSourceConfig
    targetConfig.setDataSourceType(DataSourceType.VARIABLES)
    step.setTargetConfig(targetConfig)

    val selectTargetStep = steps.last

    val selectTargetStepConfig = selectTargetStep.target.asInstanceOf[DBDataSourceConfig]

    val joinCondition = dwdModding.columns.filter(_.logicPrimaryColumn)
      .map { col =>
        s"dwd.${col.targetColumn} = incremental_data.${col.targetColumn}"
      }.mkString(" and ")

    val whereCondition = dwdModding.columns.filter(_.logicPrimaryColumn)
      .map { col =>
        s"incremental_data.${col.targetColumn} is not null"
      }.mkString(" and ")

    assert(!isNullOrEmpty(whereCondition))

    val partitionCondition = dwdModding.columns.filter(_.partitionColumn).map(_.targetColumn)
      .map(column => {
        s"concat('`$column` = ', `$column`)"
      })
      .mkString(", ")

    val selectSql =
      s"""
         |select concat('where (',
         |  ifEmpty(
         |    concat_ws(')\\n   or (', collect_set(concat_ws(' and ', $partitionCondition))),
         |    '1 = 1'),
         |  ')') as `DWD_UPDATED_PARTITION`
         |from (
         |  select
         |  dwd.*
         |  from ${quote(sourceConfig.dbName, sourceType)}.${quote(sourceConfig.tableName, sourceType)} dwd
         |  left join ${quote(selectTargetStepConfig.tableName, sourceType)} incremental_data on $joinCondition
         |  where $whereCondition
         |        ${if (isSCD) "and dwd.is_latest = 1" else "and '1' = '1'"}
         |)
         |""".stripMargin

    step.setSqlTemplate(selectSql)


    steps :+ step
  }

  //TODO: refactor
  def genDwdPartitionClauseStep(steps: List[WorkflowStep], dimTable: DimTable, stepIndex: Int): List[WorkflowStep] = {
    val sourceType: String = SPARK_SQL

    val step = new WorkflowStep

    step.setStep(stepIndex.toString)

    val sourceConfig = new DBDataSourceConfig
    val any = dimTable.cols.head
    sourceConfig.setDataSourceType(any.joinDbType)
    sourceConfig.setDbName(any.joinDb)
    sourceConfig.setTableName(any.joinTable)
    step.setSourceConfig(sourceConfig)

    val targetConfig = new DBDataSourceConfig
    targetConfig.setDataSourceType(DataSourceType.VARIABLES)
    step.setTargetConfig(targetConfig)

    val selectTargetStep = steps.last

    val selectTargetStepConfig = selectTargetStep.target.asInstanceOf[DBDataSourceConfig]

    val joinCondition = dimTable.joinOnColumns
      .map { col =>
        s"dwd.${col.joinTableColumn} = incremental_data.${col.joinTableColumn}"
      }.mkString(" and ")

    val whereCondition = dimTable.joinOnColumns
      .map { col =>
        s"incremental_data.${col.joinTableColumn} is not null"
      }.mkString(" and ")

    assert(!isNullOrEmpty(whereCondition))

    val partitionCondition = dimTable.partitionCols.map(_.sourceColumn)
      .map(column => {
        s"concat('`$column` = ', `$column`)"
      })
      .mkString(", ")

    val selectSql =
      s"""
         |select concat('where (',
         |  ifEmpty(
         |    concat_ws(')\\n   or (', collect_set(concat_ws(' and ', $partitionCondition))),
         |    '1 = 1'),
         |  ')') as `DWD_UPDATED_PARTITION`
         |from (
         |  select dwd.*
         |  from ${quote(sourceConfig.dbName, sourceType)}.${quote(sourceConfig.tableName, sourceType)} dwd
         |  left join ${quote(selectTargetStepConfig.tableName, sourceType)} incremental_data on $joinCondition
         |  where $whereCondition
         |        and dwd.is_latest = 1
         |)
         |""".stripMargin

    step.setSqlTemplate(selectSql)


    steps :+ step
  }


  //TODO: refactor
  def genDwdViewStep(steps: List[WorkflowStep], dimTable: DimTable, stepIndex: Int): List[WorkflowStep] = {
    val sourceType: String = SPARK_SQL

    val step = new WorkflowStep

    step.setStep(stepIndex.toString)

    val sourceConfig = new DBDataSourceConfig
    val any = dimTable.cols.head
    sourceConfig.setDataSourceType(any.joinDbType)
    sourceConfig.setDbName(any.joinDb)
    sourceConfig.setTableName(any.joinTable)
    step.setSourceConfig(sourceConfig)

    val targetConfig = new DBDataSourceConfig
    targetConfig.setDataSourceType(DataSourceType.TEMP)
    targetConfig.setTableName(getTempName(sourceConfig.tableName, "changed_partition_view"))
    step.setTargetConfig(targetConfig)

    val selectSql =
      s"""
         |select *
         |from ${quote(sourceConfig.dbName, sourceType)}.${quote(sourceConfig.tableName, sourceType)}
         |$${DWD_UPDATED_PARTITION}
         |""".stripMargin

    step.setSqlTemplate(selectSql)



    steps :+ step
  }

  def genScdStep(steps: List[WorkflowStep],
                 dwdModding: DwdModeling,
                 dimTable: DimTable,
                 stepIndex: Int,
                 odsView: String,
                 dwView: String): List[WorkflowStep] = {

    val any = dimTable.cols.head

    val step = new WorkflowStep

    step.setStep(stepIndex.toString)

    val sourceConfig = new TransformationDataSourceConfig
    sourceConfig.setDataSourceType(DataSourceType.TRANSFORMATION)
    sourceConfig.className = "com.github.sharpdata.sharpetl.spark.transformation.SCDTransformer"
    sourceConfig.methodName = "transform"
    sourceConfig.args = Map(
      canNotBeEmpty("odsViewName", odsView),
      canNotBeEmpty("dwViewName", dwView),
      canNotBeEmpty("primaryFields", dimTable.joinOnColumns.map(_.joinTableColumn).mkString(",")),
      //canNotBeEmpty("partitionField", ""),
      //canNotBeEmpty("partitionFormat", ""),
      canNotBeEmpty("updateTimeField", dimTable.updateTimeCols.map(_.sourceColumn).head),
      canNotBeEmpty("createTimeField", dimTable.createTimeCols.map(_.sourceColumn).head),
      canNotBeEmpty("dropUpdateTimeField", "true"),
      canNotBeEmpty("dwUpdateType", if (dwdModding.dwdTableConfig.updateType == "full") LoadType.FULL else LoadType.INCREMENTAL),
      canNotBeEmpty("timeFormat", "yyyy-MM-dd HH:mm:ss"),
      ("surrogateField", getSurrogateField(dwdModding, any.joinTable))
    )
    step.setSourceConfig(sourceConfig)

    val targetConfig = new DBDataSourceConfig
    targetConfig.setDataSourceType(any.joinDbType)
    targetConfig.setDbName(any.joinDb)
    targetConfig.setTableName(any.joinTable)
    step.setTargetConfig(targetConfig)

    step.writeMode = WriteMode.OVER_WRITE



    steps :+ step
  }

  private def getSurrogateField(dwdModding: DwdModeling, targetTable: String): String = {
    val cols = dwdModding.columns
      .filter(it => isNullOrEmpty(it.sourceColumn) && it.extraColumnExpression == ZIP_ID_FLAG)
      .filter(it => it.joinTable == targetTable || (it.targetTable == targetTable && isNullOrEmpty(it.joinTable)))
    if (cols.isEmpty) {
      ""
    } else {
      assert(cols.size == 1)
      targetColOrSourceCol(cols.head)
    }
  }
}
