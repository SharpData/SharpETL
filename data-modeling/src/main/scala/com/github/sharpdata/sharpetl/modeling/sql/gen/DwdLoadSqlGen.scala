package com.github.sharpdata.sharpetl.modeling.sql.gen

import com.github.sharpdata.sharpetl.modeling.excel.model.DwdModeling
import com.google.gson.JsonObject
import com.github.sharpdata.sharpetl.core.datasource.config.{DBDataSourceConfig, TransformationDataSourceConfig}
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.core.util.Constants.{IncrementalType, LoadType, DataSourceType}
import com.github.sharpdata.sharpetl.core.util.StringUtil.{canNotBeEmpty, isNullOrEmpty}
import com.github.sharpdata.sharpetl.modeling.excel.model.FactOrDim.DIM
import DwdExtractSqlGen.ZIP_ID_FLAG

import scala.collection.mutable

object DwdLoadSqlGen {
  def genLoadStep(steps: List[WorkflowStep], dwdModding: DwdModeling, stepIndex: Int): List[WorkflowStep] = {
    val factOrDim = dwdModding.dwdTableConfig.factOrDim
    var isSlowChanging = false
    if (DIM.equals(factOrDim)) {
      isSlowChanging = true
    } else {
      isSlowChanging = false
    }
    isSlowChanging = dwdModding.dwdTableConfig.slowChanging

    val currentDb = dwdModding.dwdTableConfig.targetDb
    val currentDbType = dwdModding.dwdTableConfig.targetType
    val currentTable = dwdModding.dwdTableConfig.targetTable

    val updateTable = steps.last.target.asInstanceOf[DBDataSourceConfig].tableName

    val sourceType: String = dwdModding.dwdTableConfig.sourceType

    val step = new WorkflowStep

    step.setStep(stepIndex.toString)

    val sourceConfig = new TransformationDataSourceConfig
    sourceConfig.setDataSourceType(DataSourceType.TRANSFORMATION)
    sourceConfig.className = "com.github.sharpdata.sharpetl.spark.transformation.JdbcLoadTransformer"
    sourceConfig.methodName = "transform"
    //    TODO check:有targetColumn必须有targetColumnType，此处去掉了zipIdColumn
    val columnTypeMap = new JsonObject()
    dwdModding.columns
      .filterNot(column => isNullOrEmpty(column.targetColumn))
      .filterNot(column => ZIP_ID_FLAG.equals(column.extraColumnExpression) && isNullOrEmpty(column.joinTable))
      .foreach {
        column => {
          columnTypeMap.addProperty(column.targetColumn, column.targetColumnType)
        }
      }

    sourceConfig.args = Map(
      canNotBeEmpty("updateType", if (dwdModding.dwdTableConfig.loadType == "full") LoadType.FULL else LoadType.INCREMENTAL),
      canNotBeEmpty("slowChanging", isSlowChanging.toString),
      canNotBeEmpty("updateTable", updateTable),
      canNotBeEmpty("currentDb", currentDb),
      canNotBeEmpty("currentDbType", currentDbType),
      canNotBeEmpty("currentTable", currentTable),
      canNotBeEmpty("primaryFields", dwdModding.columns.filter(_.logicPrimaryColumn).map(_.targetColumn).mkString(",")),
      //      TODO create time和update time不关心多列的情况
      canNotBeEmpty("businessCreateTime", dwdModding.columns.filter(_.businessCreateTime).map(_.targetColumn).head),
      canNotBeEmpty("businessUpdateTime", dwdModding.columns.filter(_.businessUpdateTime).map(_.targetColumn).head),
      canNotBeEmpty("currentTableColumnsAndType", columnTypeMap.toString())
    )
    //    if (isSlowChanging) {
    //      //      TODO check:渐变时，一定要满足有zip_id_flag标识的column，且jointable为空。且该column唯一。
    //      source.args += ("currentTableZipColumn" -> dwdModding.columns
    //        .filter(it => ZIP_ID_FLAG.equals(it.extraColumnExpression) && isNullOrEmpty(it.joinTable))
    //        .map(_.targetColumn).head)
    //    }
    step.setSourceConfig(sourceConfig)

    val targetConfig = new DBDataSourceConfig
    targetConfig.setDataSourceType(DataSourceType.DO_NOTHING)
    step.setTargetConfig(targetConfig)
    steps :+ step
  }

}
