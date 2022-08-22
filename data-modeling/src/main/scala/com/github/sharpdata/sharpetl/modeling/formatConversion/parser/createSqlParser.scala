package com.github.sharpdata.sharpetl.modeling.formatConversion

import com.github.sharpdata.sharpetl.core.util.ExcelUtil.{getBoolCell, getStringCellOrNull, readHeaders, readSheet}
import com.github.sharpdata.sharpetl.modeling.excel.model.OdsModelingSheetHeader.{COLUMN_TYPE, EXTRA_COLUMN_EXPRESSION, INCREMENTAL_COLUMN,
  ODS_MODELING_SHEET_NAME, PRIMARY_COLUMN, SOURCE_COLUMN, TARGET_COLUMN}
import com.github.sharpdata.sharpetl.modeling.excel.model.OdsTable.OdsModelingColumn
import com.github.sharpdata.sharpetl.modeling.excel.model.OdsTableConfigSheetHeader
import com.github.sharpdata.sharpetl.modeling.excel.model.OdsTableConfigSheetHeader.{ODS_TABLE_CONFIG_SHEET_NAME, SOURCE_TABLE, TARGET_TABLE, TARGET_TYPE}
import com.github.sharpdata.sharpetl.modeling.formatConversion.model.TableConfigSheetHeader
import org.apache.poi.ss.usermodel.Row

object createSqlParser {
  val typeConversionFilePath: String = this
    .getClass
    .getClassLoader
    .getResource("dbToMiddleLevel.xlsx")
    .getPath

  val typeTestPath: String = "~/Desktop/dbToMiddleLevel.xlsx"

  def readDbFieldType(filePath: String): (Map[String, String], Map[String, String]) = {
    val tableConfigSheet = readSheet(filePath, ODS_TABLE_CONFIG_SHEET_NAME)
    implicit val headersOds: Map[String, Int] = readHeaders(tableConfigSheet.head)
    val (sourceDbType, targetDbType) = tableConfigSheet
      .tail
      .map(rowExtractDdType(headersOds)).head
    (readTableConfig(sourceDbType), readTableConfig(targetDbType))
  }

  def createTableDDLList(filePath: String): List[(String, String)] = {
    val sourceFieldTypeList = getSourceFieldType(filePath)
    sourceFieldTypeList.map(it => {
      val targetTableName = it._1._2
      val odsModelingColumnSeq = it._2
      (targetTableName, createTableDDL(targetTableName, odsModelingColumnSeq, filePath))
    }).toList
  }


  def createTableDDL(targetTableName:String, odsModelingColumnSeq: Seq[OdsModelingColumn],filePath:String):String= {
    val targetColumns = odsModelingColumnSeq.map(_.targetColumn)
    val fromSource = odsModelingColumnSeq.map(_.sourceType)
    val (sourceToMiddle, middleToTarget) = readDbFieldType(filePath)
    val sourceTypeAndLength = fromSource.map(it => {
      val sourceTypeList = it.split('(')
      val typeLength: String = it.replace(sourceTypeList(0), "")
      (sourceTypeList(0), typeLength)
    })
    sqlCreate(targetTableName,sourceTypeAndLength, sourceToMiddle, middleToTarget,targetColumns)
  }

  def sqlCreate(targetTableName:String ,sourceTypeAndLength: Seq[(String, String)], sourceToMiddle: Map[String, String],
                middleToTarget: Map[String, String],targetColumns:Seq[String]):String = {
    val fromSourceType = sourceTypeAndLength.map(_._1)
    val fromSourceLength = sourceTypeAndLength.map(_._2)
    val targetColWithType =  fromSourceType
      .map(sourceToMiddle.get(_).head)
      .map(middleToTarget.get(_).head)
      .zip(fromSourceLength)
      .map(it => s"""${it._1}${it._2}""")
      .zip(targetColumns)
    val columns = targetColWithType.map(it => s"${it._2}  ${it._1}").mkString(",\n")
    s"""create table $targetTableName(
        |=$columns
        |)""".stripMargin
  }

  def getSourceFieldType(filePath: String): Map[(String,String),Seq[OdsModelingColumn]] = {
    val modelingSheet = readSheet(filePath, ODS_MODELING_SHEET_NAME)
    implicit val headers: Map[String, Int] = readHeaders(modelingSheet.head)
    modelingSheet
      .tail
      .map(row => OdsModelingColumn(
        sourceTable = getStringCellOrNull(SOURCE_TABLE, row),
        targetTable = getStringCellOrNull(TARGET_TABLE, row),
        sourceColumn = getStringCellOrNull(SOURCE_COLUMN, row),
        targetColumn = getStringCellOrNull(TARGET_COLUMN, row),
        sourceType = getStringCellOrNull(COLUMN_TYPE, row),
        extraColumnExpression = getStringCellOrNull(EXTRA_COLUMN_EXPRESSION, row),
        incrementalColumn = getBoolCell(INCREMENTAL_COLUMN, row),
        primaryKeyColumn = getBoolCell(PRIMARY_COLUMN, row)))
      .groupBy(it=>(it.sourceTable,it.targetTable))
  }

  def readTableConfig(dbName: String): Map[String, String] = {
    val ModelingSheet = readSheet(typeTestPath, dbName)
    implicit val headers: Map[String, Int] = readHeaders(ModelingSheet.head)
    ModelingSheet
      .tail
      .map(rowToColumn).toMap
  }

  private def rowToColumn(implicit headers: Map[String, Int]): Row => (String, String) = {
    row =>
      (
        getStringCellOrNull(TableConfigSheetHeader.SOURCE, row),
        getStringCellOrNull(TableConfigSheetHeader.TARGET, row)
      )
  }

  private def rowExtractDdType(implicit headers: Map[String, Int]): Row => (String, String) = {
    row =>
      (s"""${getStringCellOrNull(OdsTableConfigSheetHeader.SOURCE_TYPE, row)}ToMiddle""",
        s"""MiddleTo${getStringCellOrNull(OdsTableConfigSheetHeader.TARGET_TYPE, row)}"""
      )
  }
}
