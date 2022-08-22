package com.github.sharpdata.sharpetl.modeling.excel.parser

import com.github.sharpdata.sharpetl.modeling.excel.model.OdsTableConfigSheetHeader
import com.github.sharpdata.sharpetl.core.util.ETLLogger
import com.github.sharpdata.sharpetl.core.util.ExcelUtil._
import com.github.sharpdata.sharpetl.modeling.Exception.TableConfigHasDuplicateSourceAndTargetTableException
import com.github.sharpdata.sharpetl.modeling.excel.model.DwdTableConfigSheetHeader.{DEFAULT_START, DEPENDS_ON, LOAD_TYPE, LOG_DRIVEN_TYPE, UPSTREAM}
import com.github.sharpdata.sharpetl.modeling.excel.model.OdsModelingSheetHeader._
import com.github.sharpdata.sharpetl.modeling.excel.model.OdsTable._
import com.github.sharpdata.sharpetl.modeling.excel.model.OdsTableConfigSheetHeader._
import org.apache.poi.ss.usermodel.Row

object OdsTableParser {
  def readOdsConfig(filePath: String): Seq[OdsModeling] = {

    val dwdModelingSheet = {
      val modelingSheet = readSheet(filePath, ODS_MODELING_SHEET_NAME)
      implicit val headers: Map[String, Int] = readHeaders(modelingSheet.head)
      modelingSheet
        .tail
        .map(rowToOdsColumnEtl)
    }

    def readOdsModelingSheet(sourceTable: String, targetTable: String): Seq[OdsModelingColumn] = {
      dwdModelingSheet.filter(column => column.sourceTable == sourceTable && column.targetTable == targetTable)
    }

    def readOdsTableConfigSheet: Seq[OdsTableConfig] = {
      val tableConfigSheet = readSheet(filePath, ODS_TABLE_CONFIG_SHEET_NAME)
      implicit val headers: Map[String, Int] = readHeaders(tableConfigSheet.head)
      val tableConfigs = tableConfigSheet
        .tail
        .map(rowToOdsTableEtl)
      tableConfigs
        .groupBy(table => (table.sourceTable, table.targetTable))
        .foreach { // just for check duplicated table config
          case ((_, _), tables) =>
            if (tables.size > 1) {
              val errorMsg: String = s"In $ODS_TABLE_CONFIG_SHEET_NAME sheet, " +
                s"the ${SOURCE_TABLE} and ${TARGET_TABLE} config is duplicated."
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

    readOdsTableConfigSheet
      .map {
        it => OdsModeling(it, readOdsModelingSheet(it.sourceTable, it.targetTable))
      }
  }

  private def rowToOdsTableEtl(implicit headers: Map[String, Int]): Row => OdsTableConfig = {
    row =>
      OdsTableConfig(
        sourceConnection = getStringCellOrNull(SOURCE_CONNECTION, row),
        sourceType = getStringCellOrNull(SOURCE_TYPE, row),
        sourceDb = getStringCellOrNull(OdsTableConfigSheetHeader.SOURCE_DB, row),
        sourceTable = getStringCellOrNull(OdsTableConfigSheetHeader.SOURCE_TABLE, row),
        targetConnection = getStringCellOrNull(TARGET_CONNECTION, row),
        targetType = getStringCellOrNull(TARGET_TYPE, row),
        targetDb = getStringCellOrNull(OdsTableConfigSheetHeader.TARGET_DB, row),
        targetTable = getStringCellOrNull(OdsTableConfigSheetHeader.TARGET_TABLE, row),
        filterExpression = getStringCellOrNull(FILTER_EXPR, row),
        loadType = getStringCellOrNull(LOAD_TYPE, row),
        logDrivenType = getStringCellOrNull(LOG_DRIVEN_TYPE,row),
        upstream = getStringCellOrNull(UPSTREAM,row),
        dependsOn = getStringCellOrNull(DEPENDS_ON,row),
        defaultStart = getStringCellOrNull(DEFAULT_START,row),
        partitionFormat = getStringCellOrNull(PARTITION_FORMAT, row),
        timeFormat = getStringCellOrDefault(TIME_FORMAT, row, "YYYY-MM-DD hh:mm:ss"),
        period = getNumericCell(PERIOD, row).toInt.toString
      )
  }

  private def rowToOdsColumnEtl(implicit headers: Map[String, Int]): Row => OdsModelingColumn = {
    row =>
      OdsModelingColumn(
        sourceTable = getStringCellOrNull(SOURCE_TABLE, row),
        targetTable = getStringCellOrNull(TARGET_TABLE, row),
        sourceColumn = getStringCellOrNull(SOURCE_COLUMN, row),
        targetColumn = getStringCellOrNull(TARGET_COLUMN, row),
        sourceType = getStringCellOrNull(COLUMN_TYPE,row),
        extraColumnExpression = getStringCellOrNull(EXTRA_COLUMN_EXPRESSION, row),
        incrementalColumn = getBoolCell(INCREMENTAL_COLUMN, row),
        primaryKeyColumn = getBoolCell(PRIMARY_COLUMN, row)
      )
  }
}
