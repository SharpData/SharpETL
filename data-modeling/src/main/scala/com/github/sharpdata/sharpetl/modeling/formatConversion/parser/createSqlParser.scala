package com.github.sharpdata.sharpetl.modeling.formatConversion

import com.github.sharpdata.sharpetl.core.util.ExcelUtil.{getBoolCell, getStringCellOrNull, readHeaders, readSheet}
import com.github.sharpdata.sharpetl.modeling.excel.model.OdsModelingSheetHeader.{COLUMN_TYPE, EXTRA_COLUMN_EXPRESSION, INCREMENTAL_COLUMN, IS_NULLABLE, ODS_MODELING_SHEET_NAME, PRIMARY_COLUMN, SOURCE_COLUMN, TARGET_COLUMN}
import com.github.sharpdata.sharpetl.modeling.excel.model.OdsTableConfigSheetHeader
import com.github.sharpdata.sharpetl.modeling.excel.model.OdsTableConfigSheetHeader.{ODS_TABLE_CONFIG_SHEET_NAME, SOURCE_TABLE, TARGET_TABLE, TARGET_TYPE}
import com.github.sharpdata.sharpetl.modeling.formatConversion.model.OdsTable.{OdsModelingColumnParietal, OdsTypeModelingColumn}
import com.github.sharpdata.sharpetl.modeling.formatConversion.model.{OdsTable, TableConfigSheetHeader}
import org.apache.poi.ss.usermodel.Row

object createSqlParser{
  val typeConversionFilePath: String = this
    .getClass
    .getClassLoader
    .getResource("dbToMiddleLevel.xlsx")
    .getPath

   val typeTestPath:String="~/Desktop/dbToMiddleLevel.xlsx"

  def readDbFieldType(filePath:String): (Map[String, String], Map[String, String]) ={
    val tableConfigSheet = readSheet(filePath, ODS_TABLE_CONFIG_SHEET_NAME)
    implicit val headersOds: Map[String, Int] = readHeaders(tableConfigSheet.head)
    val (sourceDbType,targetDbType) = tableConfigSheet
      .tail
      .map(rowExtractDdType(headersOds)).head
    (readTableConfig(sourceDbType) ,readTableConfig(targetDbType))
  }

  def typeConversion(filePath:String): List[((String,String),Seq[OdsTypeModelingColumn])]= {
    val sourceFieldType = getSourceFieldType(filePath)
    val dBFieldTypeTuple = readDbFieldType(filePath)
    val targetTypeList = sourceFieldType.map(it => {
    val sourceTypeList = it.sourceType.split('(')
      val middleType: String = dBFieldTypeTuple._1.get(sourceTypeList(0)).head
      val targetTypeList = dBFieldTypeTuple._2.filter(it => it._2 == middleType).keys.toList
      val targetType = if(targetTypeList.contains(sourceTypeList(0))) sourceTypeList(0) else targetTypeList.head
      OdsTypeModelingColumn(it.sourceTable,it.targetTable,it.sourceColumn,it.targetColumn,
        it.sourceType,it.sourceType.replace(sourceTypeList(0),targetType),it.extraColumnExpression,it.incrementalColumn,it.primaryKeyColumn,it.isNullAbel)}
    ).groupBy(it=>(it.sourceTable,it.targetTable)).toList
    targetTypeList
  }

  def createTableList(filePath: String): List[((String,String),String)] = {
    val odsTypeModelingColumnList = typeConversion(filePath)
    odsTypeModelingColumnList.map(it => {
      (it._1,createTable(it._1._2, it._2))}
    )
  }

  def createTable(targetTable: String, columns: Seq[OdsTable.OdsTypeModelingColumn]): String = {
    val primaryKeyField = columns.filter(it => it.primaryKeyColumn)
    val primaryKeyString= primaryKeyField.map(_.targetColumn).mkString(",")
    createTableAboutPrimaryKey(primaryKeyField.size,primaryKeyString,targetTable ,columns)
  }


  def createTableAboutPrimaryKey(primaryKeyNumber:Int, primaryKeyString:String, targetTable: String, columns: Seq[OdsTable.OdsTypeModelingColumn]): String = {
    var sql = s"""create table $targetTable\n(\n"""
    sql = sql + "       " ++ buildColumnString(primaryKeyNumber,columns)
    if(primaryKeyNumber > 1) sql = "primary key(" + primaryKeyString + ")"
    sql = sql ++ "\n);"
    sql
  }

  private def buildColumnString(number:Int,columns: Seq[OdsTable.OdsTypeModelingColumn]): String = {
    columns
      .map(col => {s"""${col.targetColumn}  ${col.targetType}""" ++ getExtraContention(number,col)}).mkString(",\n       ")
  }

  private def getExtraContention(number: Int,column: OdsTable.OdsTypeModelingColumn):String={
        var extractAddition=""
    if(column.primaryKeyColumn && number == 1) extractAddition =extractAddition + "  primary key"
    if(column.isNullAbel) extractAddition =extractAddition + "  not null"
    if(column.incrementalColumn) extractAddition = extractAddition + "  auto_increment"
    extractAddition
  }

  def getSourceFieldType(filePath: String): Seq[OdsModelingColumnParietal] = {
    val modelingSheet = readSheet(filePath, ODS_MODELING_SHEET_NAME)
    implicit val headers: Map[String, Int] = readHeaders(modelingSheet.head)
    modelingSheet
      .tail
      .map(row => OdsModelingColumnParietal(sourceTable = getStringCellOrNull(SOURCE_TABLE, row),
        targetTable = getStringCellOrNull(TARGET_TABLE, row),
        sourceColumn = getStringCellOrNull(SOURCE_COLUMN, row),
        targetColumn = getStringCellOrNull(TARGET_COLUMN, row),
        sourceType = getStringCellOrNull(COLUMN_TYPE,row),
        extraColumnExpression = getStringCellOrNull(EXTRA_COLUMN_EXPRESSION, row),
        incrementalColumn = getBoolCell(INCREMENTAL_COLUMN, row),
        primaryKeyColumn = getBoolCell(PRIMARY_COLUMN, row),
        isNullAbel = getBoolCell(IS_NULLABLE,row)))
  }

  def readTableConfig(dbName: String):Map[String,String]= {
    val ModelingSheet = readSheet(typeTestPath, dbName)
    implicit val headers: Map[String, Int] = readHeaders(ModelingSheet.head)
    ModelingSheet
      .tail
      .map(rowToColumn).toMap
  }

  private def rowToColumn(implicit headers: Map[String, Int]):Row=>(String,String) = {
    row =>(
        getStringCellOrNull(TableConfigSheetHeader.SOURCE, row),
        getStringCellOrNull(TableConfigSheetHeader.TARGET, row)
      )
  }
  private def rowExtractDdType(implicit headers: Map[String, Int]): Row => (String,String) = {
    row =>(getStringCellOrNull(OdsTableConfigSheetHeader.SOURCE_TYPE, row),
          getStringCellOrNull(OdsTableConfigSheetHeader.TARGET_TYPE, row)
      )
  }
}
