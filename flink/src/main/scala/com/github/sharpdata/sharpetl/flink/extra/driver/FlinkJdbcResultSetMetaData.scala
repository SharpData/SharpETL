package com.github.sharpdata.sharpetl.flink.extra.driver

import com.github.sharpdata.sharpetl.flink.job.Types.DataFrame
import com.github.sharpdata.sharpetl.flink.util.ETLFlinkSession

import java.sql.ResultSetMetaData
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

// scalastyle:off
class FlinkJdbcResultSetMetaData(val data: DataFrame) extends ResultSetMetaData{
  override def getColumnCount: Int = getFields.size

  private def getFields = {
    data.getResolvedSchema.getColumns.map(_.getName).toList
  }

  override def isAutoIncrement(column: Int): Boolean = false

  override def isCaseSensitive(column: Int): Boolean = false

  override def isSearchable(column: Int): Boolean = true

  override def isCurrency(column: Int): Boolean = false

  override def isNullable(column: Int): Int = 0

  override def isSigned(column: Int): Boolean = false

  override def getColumnDisplaySize(column: Int): Int = getFields(column - 1).length

  override def getColumnLabel(column: Int): String = getFields(column - 1)

  override def getColumnName(column: Int): String = getFields(column - 1)

  override def getSchemaName(column: Int): String = getFields(column - 1)

  override def getPrecision(column: Int): Int = 0

  override def getScale(column: Int): Int = 0

  override def getTableName(column: Int): String = ""

  override def getCatalogName(column: Int): String = ""

  override def getColumnType(column: Int): Int = 0

  override def getColumnTypeName(column: Int): String = data.getResolvedSchema.getColumns.map(_.getDataType.toString).toList(column - 1)

  override def isReadOnly(column: Int): Boolean = false

  override def isWritable(column: Int): Boolean = true

  override def isDefinitelyWritable(column: Int): Boolean = true

  override def getColumnClassName(column: Int): String = ""

  override def unwrap[T](iface: Class[T]): T = ???

  override def isWrapperFor(iface: Class[_]): Boolean = ???
}

// scalastyle:on