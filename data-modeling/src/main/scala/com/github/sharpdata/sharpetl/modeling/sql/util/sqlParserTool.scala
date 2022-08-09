package com.github.sharpdata.sharpetl.modeling.sql.util

import com.github.sharpdata.sharpetl.core.util.StringUtil.isNullOrEmpty
import com.github.sharpdata.sharpetl.modeling.sql.dialect.SqlDialect.quote

object sqlParserTool {
  def getRowFilterAsString(rowFilterExpression: String, sourceType: String,etlLevel:String): String = {
    val sql=if( isNullOrEmpty(rowFilterExpression)) {
      ""
    } else {
     rowFilterExpression.replace("= ", "=").replace(" =", "=").split(" ").map(it =>
        getRowFilterAsPartial(it, sourceType,etlLevel)).mkString(" ")
    }
     if(etlLevel.equals("ods")) "AND " + sql else "and " + sql
  }

  def getRowFilterAsPartial(rowFilterPartial: String, sourceType: String,etlLevel:String): String = {
    if (rowFilterPartial.contains("(")) {
      val partial = getRowFilterAsPartialString(rowFilterPartial.substring(1), sourceType)
      s"""($partial"""
    } else if (rowFilterPartial.contains(")")) {
      val partial = getRowFilterAsPartialString(rowFilterPartial.substring(0, rowFilterPartial.length - 1), sourceType)
      s"""$partial)"""
    } else if (rowFilterPartial.contains("=")) {
      getRowFilterAsPartialString(rowFilterPartial, sourceType)
    } else {

    val value=if(etlLevel.equals("ods"))  rowFilterPartial.toUpperCase() else rowFilterPartial.toLowerCase()
      println(value)
      s"""$value"""
    }
  }

  def getRowFilterAsPartialString(rowFilterExpression: String, sourceType: String): String = {
    val rowFilterExpressionArray = rowFilterExpression.split("=")
    val colum: String = rowFilterExpressionArray(0)
    val columnValue = rowFilterExpressionArray(1)
    if(columnValue.contains("'")) s"""${quote(colum, sourceType)} = $columnValue """ else s"""${quote(colum, sourceType)} = '$columnValue' """
  }
}
