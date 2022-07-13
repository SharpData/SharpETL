package com.github.sharpdata.sharpetl.modeling.sql.gen

import com.github.sharpdata.sharpetl.modeling.excel.model.{CreateDimMode, DwdModeling}
import com.google.gson.JsonObject
import com.github.sharpdata.sharpetl.core.datasource.config.{DBDataSourceConfig, TransformationDataSourceConfig}
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.core.util.Constants.{IncrementalType, DataSourceType}
import com.github.sharpdata.sharpetl.core.util.StringUtil.{canNotBeEmpty, getTempName, isNullOrEmpty}
import DwdExtractSqlGen.getTargetColumn

import scala.collection.mutable

// scalastyle:off
object AutoCreateDimSqlGen2 {
  def genAutoCreateDimStep(steps: List[WorkflowStep], dwdModding: DwdModeling, stepIndex: Int): (List[WorkflowStep], Int) = {
    var index = stepIndex - 1
    val stepsTemp: List[WorkflowStep] = dwdModding.columns
      .filterNot(column => isNullOrEmpty(column.joinTable))
      .filter(column => CreateDimMode.ALWAYS.equals(column.createDimMode) || CreateDimMode.ONCE.equals(column.createDimMode))
      .groupBy(column => (column.joinDbType, column.joinDb, column.joinTable))
      .toList
      .sortBy(_._1._3)
      .map {
        case ((joinDbType, joinDb, joinTable), dimColumns) => {
          val createDimMode = dimColumns.head.createDimMode
          val updateTable = getTempName(dwdModding.dwdTableConfig.sourceTable, "extracted")
          val dimDbType = joinDbType
          val dimDb = joinDb
          val dimTable = joinTable
          val currentBusinessCreateTime = getTargetColumn(dwdModding.columns.filter(_.businessCreateTime).head)

          val dimTableColumnsAndType = new JsonObject()
          val currentAndDimColumnsMapping = new JsonObject()
          val currentAndDimPrimaryMapping = new JsonObject()
          //          TODO 缺少维度表的创建时间配置，此处手动添加
          dimTableColumnsAndType.addProperty("create_time", "timestamp")
          currentAndDimColumnsMapping.addProperty("order_create_time", "create_time")
          dimColumns
            .filterNot(column => isNullOrEmpty(column.joinTableColumn))
            .filter(column => isNullOrEmpty(column.extraColumnExpression))
            .foreach {
              column => {
                dimTableColumnsAndType.addProperty(column.joinTableColumn, column.targetColumnType)
                currentAndDimColumnsMapping.addProperty(getTargetColumn(column), column.joinTableColumn)
                if (!isNullOrEmpty(column.joinOn)) {
                  currentAndDimPrimaryMapping.addProperty(getTargetColumn(column), column.joinTableColumn)
                }
              }
            }

          val step = new WorkflowStep
          index = index + 1
          step.setStep(index.toString)

          val sourceConfig = new TransformationDataSourceConfig
          sourceConfig.setDataSourceType(DataSourceType.TRANSFORMATION)
          sourceConfig.className = "com.github.sharpdata.sharpetl.spark.transformation.JdbcAutoCreateDimTransformer"
          sourceConfig.methodName = "transform"

          sourceConfig.args = Map(
            canNotBeEmpty("updateTable", updateTable),
            canNotBeEmpty("createDimMode", createDimMode),
            canNotBeEmpty("dimDbType", dimDbType),
            canNotBeEmpty("dimDb", dimDb),
            canNotBeEmpty("dimTable", dimTable),
            canNotBeEmpty("currentBusinessCreateTime", currentBusinessCreateTime),
            canNotBeEmpty("dimTableColumnsAndType", dimTableColumnsAndType.toString()),
            canNotBeEmpty("currentAndDimColumnsMapping", currentAndDimColumnsMapping.toString()),
            canNotBeEmpty("currentAndDimPrimaryMapping", currentAndDimPrimaryMapping.toString())
          )
          step.setSourceConfig(sourceConfig)

          val targetConfig = new DBDataSourceConfig
          targetConfig.setDataSourceType(DataSourceType.DO_NOTHING)
          step.setTargetConfig(targetConfig)

          step
        }
      }
    (steps ++ stepsTemp, index + 1)
  }
}
