package com.github.sharpdata.sharpetl.spark.extra.driver

import org.apache.spark.sql.DataFrame

import java.sql.ResultSetMetaData

class SparkJdbcResultSetMetaData(val data: DataFrame) extends ResultSetMetaData{
  override def getColumnCount: Int = data.columns.length

  override def isAutoIncrement(column: Int): Boolean = false

  override def isCaseSensitive(column: Int): Boolean = false

  override def isSearchable(column: Int): Boolean = true

  override def isCurrency(column: Int): Boolean = false

  override def isNullable(column: Int): Int = 0

  override def isSigned(column: Int): Boolean = false

  override def getColumnDisplaySize(column: Int): Int = data.columns(column - 1).length

  override def getColumnLabel(column: Int): String = data.columns(column - 1)

  override def getColumnName(column: Int): String = data.columns(column - 1)

  override def getSchemaName(column: Int): String = data.columns(column - 1)

  override def getPrecision(column: Int): Int = 0

  override def getScale(column: Int): Int = 0

  override def getTableName(column: Int): String = ""

  override def getCatalogName(column: Int): String = ""

  override def getColumnType(column: Int): Int = 0

  override def getColumnTypeName(column: Int): String = data.dtypes(column - 1)._2

  override def isReadOnly(column: Int): Boolean = false

  override def isWritable(column: Int): Boolean = true

  override def isDefinitelyWritable(column: Int): Boolean = true

  override def getColumnClassName(column: Int): String = ""

  override def unwrap[T](iface: Class[T]): T = ???

  override def isWrapperFor(iface: Class[_]): Boolean = ???
}
