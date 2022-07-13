package com.github.sharpdata.sharpetl.core.util

import com.github.sharpdata.sharpetl.core.util.Constants.PathPrefix
import com.github.sharpdata.sharpetl.core.exception.Exception.{CellNotFoundException, SheetNotFoundException}
import com.github.sharpdata.sharpetl.core.util.IOUtil.getFullPath
import org.apache.poi.ss.usermodel.{Cell, Row, Sheet}
import org.apache.poi.xssf.usermodel.XSSFWorkbook

import java.io.{File, FileInputStream}
import scala.jdk.CollectionConverters._

object ExcelUtil {
  def getBoolCell(idx: Int, line: Row): Boolean = {
    Option(line.getCell(idx)).exists(_.getBooleanCellValue)
  }

  // get boolean cell or false
  def getBoolCell(header: String, line: Row)(implicit headerMapping: Map[String, Int]): Boolean = {
    Option(getCellByName(line, header)).exists(_.getBooleanCellValue)
  }

  def getNumericCell(index: Int, line: Row): Double = {
    line.getCell(index).getNumericCellValue
  }

  def getNumericCell(header: String, line: Row)(implicit headerMapping: Map[String, Int]): Double = {
    getCellByName(line, header).getNumericCellValue
  }

  def getStringCell(header: String, line: Row)(implicit headerMapping: Map[String, Int]): String = {
    scala.util.Try {
      getCellByName(line, header).getStringCellValue
    }.toOption.orNull
  }

  def getStringCellOrNull(index: Int, line: Row): String = {
    Option(line.getCell(index)).map(_.getStringCellValue).filterNot(_.isEmpty).orNull
  }

  def getStringCellOrNull(header: String, line: Row)(implicit headerMapping: Map[String, Int]): String = {
    Option(getStringCell(header, line)).filterNot(_.isEmpty).orNull
  }

  def getStringCellOrDefault(header: String, line: Row, default: String)(implicit headerMapping: Map[String, Int]): String = {
    Option(getStringCell(header, line)).filterNot(_.isEmpty).getOrElse(default)
  }

  def getCellByName(row: Row, headerName: String)(implicit headerMapping: Map[String, Int]): Cell = {
    if (!headerMapping.isDefinedAt(headerName)) {
      throw CellNotFoundException(headerName)
    }
    row.getCell(headerMapping(headerName))
  }

  def readHeaders(headerRow: Row): Map[String, Int] = {
    headerRow
      .asScala
      .map(row => row.getStringCellValue -> row.getColumnIndex)
      .toMap
  }

  private def readSheet(sheet: Sheet): Seq[Row] = {
    sheet.iterator().asScala.filter(line => line.asScala.mkString("").nonEmpty).toList
  }

  def readSheetByName(workBook: XSSFWorkbook, sheetName: String): Seq[Row] = {
    val sheet = workBook.getSheet(sheetName)
    if (sheet == null) {
      throw SheetNotFoundException(s"Sheet name: $sheetName not found in workbook.")
    } else {
      readSheet(sheet)
    }
  }

  def readWorkBook(filePath: String): XSSFWorkbook = {
    val excelFile = if (filePath.startsWith(PathPrefix.HDFS) || filePath.startsWith(PathPrefix.DBFS)) {
      HDFSUtil.readFile(filePath)
    } else {
      val path = getFullPath(filePath)
      new FileInputStream(new File(path))
    }
    new XSSFWorkbook(excelFile)
  }

  def readSheet(filePath: String, sheetName: String): Seq[Row] = {
    readSheetByName(readWorkBook(filePath), sheetName)
  }
}
