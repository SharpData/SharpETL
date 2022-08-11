package com.github.sharpdata.sharpetl.modeling.excel.parser

import com.github.sharpdata.sharpetl.core.util.ETLLogger
import com.github.sharpdata.sharpetl.core.util.ExcelUtil._
import com.github.sharpdata.sharpetl.modeling.Exception.TableConfigHasDuplicateSourceAndTargetTableException
import com.github.sharpdata.sharpetl.modeling.excel.model.DwdModelingSheetHeader._
import com.github.sharpdata.sharpetl.modeling.excel.model.{DwdModeling, DwdModelingColumn, DwdModelingSheetHeader, DwdTableConfig, DwdTableConfigSheetHeader}
import com.github.sharpdata.sharpetl.modeling.excel.model.DwdTableConfigSheetHeader._
import com.github.sharpdata.sharpetl.modeling.excel.model._
import org.apache.poi.ss.usermodel.Row

/**
 * 支持配置文件中 dwd_etl_config sheet有多个table，多行table的source_table和target_table不重复
 * 支持配置文件中 dwd_config sheet有多个table etl 配置
 * dwd_etl_config 和 dwd_config 两个sheet根据source_table和target_table关联
 */
object DwdTableParser {
  def readDwdConfig(filePath: String): Seq[DwdModeling] = {

    val dwdModelingSheet = {
      val modelingSheet = readSheet(filePath, DWD_MODELING_SHEET_NAME)
      implicit val headers: Map[String, Int] = readHeaders(modelingSheet.head)
      modelingSheet
        .tail
        .map(rowToDwdColumnEtl)
    }

    def readDwdModelingSheet(sourceTable: String, targetTable: String): Seq[DwdModelingColumn] = {
      dwdModelingSheet.filter(column => column.sourceTable == sourceTable && column.targetTable == targetTable)
    }

    def readDwdTableConfigSheet: Seq[DwdTableConfig] = {
      val tableConfigSheet = readSheet(filePath, DWD_TABLE_CONFIG_SHEET_NAME)
      implicit val headers: Map[String, Int] = readHeaders(tableConfigSheet.head)
      val tableConfigs = tableConfigSheet
        .tail
        .map(rowToDwdTableEtl)
      tableConfigs
        .groupBy(table => (table.sourceTable, table.targetTable))
        .foreach { // just for check duplicated table config
          case ((_, _), tables) =>
            if (tables.size > 1) {
              val errorMsg: String = s"In $DWD_TABLE_CONFIG_SHEET_NAME sheet, " +
                s"the ${DwdModelingSheetHeader.SOURCE_TABLE} and ${DwdModelingSheetHeader.TARGET_TABLE} config is duplicated."
              ETLLogger.error(
                s"""
                   |error message:
                   |$errorMsg
                   |""".stripMargin)
              throw TableConfigHasDuplicateSourceAndTargetTableException(errorMsg)
            }
        }
      tableConfigs
    }

    readDwdTableConfigSheet
      .map {
        it => DwdModeling(it, readDwdModelingSheet(it.sourceTable, it.targetTable))
      }
  }

  private def rowToDwdTableEtl(implicit headers: Map[String, Int]): Row => DwdTableConfig = {
    row =>
      DwdTableConfig(
        sourceConnection = getStringCellOrNull(SOURCE_CONNECTION, row),
        sourceType = getStringCellOrNull(SOURCE_TYPE, row),
        sourceDb = getStringCellOrNull(DwdTableConfigSheetHeader.SOURCE_DB, row),
        sourceTable = getStringCellOrNull(DwdTableConfigSheetHeader.SOURCE_TABLE, row),
        targetConnection = getStringCellOrNull(TARGET_CONNECTION, row),
        targetType = getStringCellOrNull(TARGET_TYPE, row),
        targetDb = getStringCellOrNull(DwdTableConfigSheetHeader.TARGET_DB, row),
        targetTable = getStringCellOrNull(DwdTableConfigSheetHeader.TARGET_TABLE, row),
        factOrDim = getStringCellOrNull(FACT_OR_DIM, row),
        slowChanging = getBoolCell(SLOW_CHANGING, row),
        rowFilterExpression = getStringCellOrNull(ROW_FILTER_EXPRESSION, row),
        loadType = getStringCellOrNull(LOAD_TYPE, row),
        logDrivenType = getStringCellOrNull(LOG_DRIVEN_TYPE,row),
        upstream = getStringCellOrNull(UPSTREAM,row),
        dependsOn = getStringCellOrNull(DEPENDS_ON,row),
        defaultStart = getStringCellOrNull(DEFAULT_START,row)
      )
  }

  private def rowToDwdColumnEtl(implicit headers: Map[String, Int]): Row => DwdModelingColumn = {
    row =>
      DwdModelingColumn(
        sourceTable = getStringCellOrNull(DwdModelingSheetHeader.SOURCE_TABLE, row),
        targetTable = getStringCellOrNull(DwdModelingSheetHeader.TARGET_TABLE, row),
        sourceColumn = getStringCellOrNull(SOURCE_COLUMN, row),
        sourceColumnDescription = getStringCellOrNull(SOURCE_COLUMN_DESCRIPTION, row),
        targetColumn = getStringCellOrNull(TARGET_COLUMN, row),
        targetColumnType = getStringCellOrNull(TARGET_COLUMN_TYPE, row),
        extraColumnExpression = getStringCellOrNull(EXTRA_COLUMN_EXPRESSION, row),
        partitionColumn = getBoolCell(PARTITION_COLUMN, row),
        logicPrimaryColumn = getBoolCell(LOGIC_PRIMARY_COLUMN, row),
        joinDbConnection = getStringCellOrNull(JOIN_DB_CONNECTION, row),
        joinDbType = getStringCellOrNull(JOIN_DB_TYPE, row),
        joinDb = getStringCellOrNull(JOIN_DB, row),
        joinTable = getStringCellOrNull(JOIN_TABLE, row),
        joinOn = getStringCellOrNull(JOIN_ON, row),
        createDimMode = getStringCellOrNull(CREATE_DIM_MODE, row),
        joinTableColumn = getStringCellOrNull(JOIN_TABLE_COLUMN, row),
        businessCreateTime = getBoolCell(BUSINESS_CREATE_TIME, row),
        businessUpdateTime = getBoolCell(BUSINESS_UPDATE_TIME, row),
        ignoreChangingColumn = getBoolCell(IGNORE_CHANGING_COLUMN, row),
        qualityCheckRules = getStringCellOrNull(QUALITY_CHECK_RULES, row)
      )
  }
}
