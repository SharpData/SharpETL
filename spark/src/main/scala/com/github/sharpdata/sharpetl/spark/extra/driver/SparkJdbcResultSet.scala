package com.github.sharpdata.sharpetl.spark.extra.driver

import org.apache.spark.sql.{DataFrame, Row}

import java.io.{InputStream, Reader}
import java.net.URL
import java.{sql, util}
import java.sql.{Blob, Clob, Date, NClob, Ref, ResultSet, ResultSetMetaData, RowId, SQLWarning, SQLXML, Statement, Time, Timestamp}
import java.time.LocalDateTime
import java.util.Calendar

// scalastyle:off
class SparkJdbcResultSet(val data: DataFrame, val statement: Statement) extends ResultSet {

  private val datas = data.toLocalIterator()
  private var currentData: Row = null

  def alreadyTheLast() = (currentData == null) || !datas.hasNext

  override def next(): Boolean = {
    val hasNext = datas.hasNext
    if (hasNext) {
      currentData = datas.next
    }
    hasNext
  }

  override def close(): Unit = ()

  override def wasNull(): Boolean = false

  override def getString(columnIndex: Int): String = currentData.get(columnIndex - 1).toString

  override def getBoolean(columnIndex: Int): Boolean = currentData.getBoolean(columnIndex - 1)

  override def getByte(columnIndex: Int): Byte = currentData.getByte(columnIndex - 1)

  override def getShort(columnIndex: Int): Short = currentData.getShort(columnIndex - 1)

  override def getInt(columnIndex: Int): Int = {
    try {
      currentData.getInt(columnIndex - 1)
    } catch {
      case _: Exception => 0
    }
  }

  override def getLong(columnIndex: Int): Long = currentData.getLong(columnIndex - 1)

  override def getFloat(columnIndex: Int): Float = currentData.getFloat(columnIndex - 1)

  override def getDouble(columnIndex: Int): Double = currentData.getDouble(columnIndex - 1)

  override def getBigDecimal(columnIndex: Int, scale: Int): java.math.BigDecimal = currentData.getDecimal(columnIndex - 1)

  override def getBytes(columnIndex: Int): Array[Byte] = Array(currentData.getByte(columnIndex - 1))

  override def getDate(columnIndex: Int): Date = currentData.getDate(columnIndex - 1)

  override def getTime(columnIndex: Int): Time = ??? //currentData.getTimestamp(columnIndex -1)

  override def getTimestamp(columnIndex: Int): Timestamp = currentData.getTimestamp(columnIndex - 1)

  override def getAsciiStream(columnIndex: Int): InputStream = ???

  override def getUnicodeStream(columnIndex: Int): InputStream = ???

  override def getBinaryStream(columnIndex: Int): InputStream = ???

  override def getString(columnLabel: String): String = currentData.getAs(columnLabel)

  override def getBoolean(columnLabel: String): Boolean = currentData.getAs(columnLabel)

  override def getByte(columnLabel: String): Byte = currentData.getAs(columnLabel)

  override def getShort(columnLabel: String): Short = currentData.getAs(columnLabel)

  override def getInt(columnLabel: String): Int = currentData.getAs(columnLabel)

  override def getLong(columnLabel: String): Long = currentData.getAs(columnLabel)

  override def getFloat(columnLabel: String): Float = currentData.getAs(columnLabel)

  override def getDouble(columnLabel: String): Double = currentData.getAs(columnLabel)

  override def getBigDecimal(columnLabel: String, scale: Int): java.math.BigDecimal = currentData.getAs(columnLabel)

  override def getBytes(columnLabel: String): Array[Byte] = currentData.getAs(columnLabel)

  override def getDate(columnLabel: String): Date = currentData.getAs(columnLabel)

  override def getTime(columnLabel: String): Time = currentData.getAs(columnLabel)

  override def getTimestamp(columnLabel: String): Timestamp = currentData.getAs(columnLabel)

  override def getAsciiStream(columnLabel: String): InputStream = ???

  override def getUnicodeStream(columnLabel: String): InputStream = ???

  override def getBinaryStream(columnLabel: String): InputStream = ???

  override def getWarnings: SQLWarning = null

  override def clearWarnings(): Unit = ()

  override def getCursorName: String = ""

  override def getMetaData: ResultSetMetaData = new SparkJdbcResultSetMetaData(data)

  override def getObject(columnIndex: Int): AnyRef = null

  override def getObject(columnLabel: String): AnyRef = null

  override def findColumn(columnLabel: String): Int = currentData.fieldIndex(columnLabel)

  override def getCharacterStream(columnIndex: Int): Reader = ???

  override def getCharacterStream(columnLabel: String): Reader = ???

  override def getBigDecimal(columnIndex: Int): java.math.BigDecimal = currentData.getDecimal(columnIndex - 1)

  override def getBigDecimal(columnLabel: String): java.math.BigDecimal = currentData.getAs(columnLabel)

  override def isBeforeFirst: Boolean = false

  override def isAfterLast: Boolean = false

  override def isFirst: Boolean = false

  override def isLast: Boolean = false

  override def beforeFirst(): Unit = ()

  override def afterLast(): Unit = ()

  override def first(): Boolean = false

  override def last(): Boolean = false

  override def getRow: Int = 0

  override def absolute(row: Int): Boolean = false

  override def relative(rows: Int): Boolean = false

  override def previous(): Boolean = false

  override def setFetchDirection(direction: Int): Unit = ()

  override def getFetchDirection: Int = 0

  override def setFetchSize(rows: Int): Unit = ()

  override def getFetchSize: Int = 0

  override def getType: Int = 0

  override def getConcurrency: Int = 0

  override def rowUpdated(): Boolean = ???

  override def rowInserted(): Boolean = ???

  override def rowDeleted(): Boolean = ???

  override def updateNull(columnIndex: Int): Unit = ()

  override def updateBoolean(columnIndex: Int, x: Boolean): Unit = ()

  override def updateByte(columnIndex: Int, x: Byte): Unit = ()

  override def updateShort(columnIndex: Int, x: Short): Unit = ()

  override def updateInt(columnIndex: Int, x: Int): Unit = ()

  override def updateLong(columnIndex: Int, x: Long): Unit = ()

  override def updateFloat(columnIndex: Int, x: Float): Unit = ()

  override def updateDouble(columnIndex: Int, x: Double): Unit = ()

  override def updateBigDecimal(columnIndex: Int, x: java.math.BigDecimal): Unit = ()

  override def updateString(columnIndex: Int, x: String): Unit = ()

  override def updateBytes(columnIndex: Int, x: Array[Byte]): Unit = ()

  override def updateDate(columnIndex: Int, x: Date): Unit = ()

  override def updateTime(columnIndex: Int, x: Time): Unit = ()

  override def updateTimestamp(columnIndex: Int, x: Timestamp): Unit = ()

  override def updateAsciiStream(columnIndex: Int, x: InputStream, length: Int): Unit = ()

  override def updateBinaryStream(columnIndex: Int, x: InputStream, length: Int): Unit = ()

  override def updateCharacterStream(columnIndex: Int, x: Reader, length: Int): Unit = ()

  override def updateObject(columnIndex: Int, x: Any, scaleOrLength: Int): Unit = ()

  override def updateObject(columnIndex: Int, x: Any): Unit = ()

  override def updateNull(columnLabel: String): Unit = ()

  override def updateBoolean(columnLabel: String, x: Boolean): Unit = ()

  override def updateByte(columnLabel: String, x: Byte): Unit = ()

  override def updateShort(columnLabel: String, x: Short): Unit = ()

  override def updateInt(columnLabel: String, x: Int): Unit = ()

  override def updateLong(columnLabel: String, x: Long): Unit = ()

  override def updateFloat(columnLabel: String, x: Float): Unit = ()

  override def updateDouble(columnLabel: String, x: Double): Unit = ()

  override def updateBigDecimal(columnLabel: String, x: java.math.BigDecimal): Unit = ()

  override def updateString(columnLabel: String, x: String): Unit = ()

  override def updateBytes(columnLabel: String, x: Array[Byte]): Unit = ()

  override def updateDate(columnLabel: String, x: Date): Unit = ()

  override def updateTime(columnLabel: String, x: Time): Unit = ()

  override def updateTimestamp(columnLabel: String, x: Timestamp): Unit = ()

  override def updateAsciiStream(columnLabel: String, x: InputStream, length: Int): Unit = ()

  override def updateBinaryStream(columnLabel: String, x: InputStream, length: Int): Unit = ()

  override def updateCharacterStream(columnLabel: String, reader: Reader, length: Int): Unit = ()

  override def updateObject(columnLabel: String, x: Any, scaleOrLength: Int): Unit = ()

  override def updateObject(columnLabel: String, x: Any): Unit = ()

  override def insertRow(): Unit = ()

  override def updateRow(): Unit = ()

  override def deleteRow(): Unit = ()

  override def refreshRow(): Unit = ()

  override def cancelRowUpdates(): Unit = ()

  override def moveToInsertRow(): Unit = ()

  override def moveToCurrentRow(): Unit = ()

  override def getStatement: Statement = statement

  override def getObject(columnIndex: Int, map: util.Map[String, Class[_]]): AnyRef = null

  override def getRef(columnIndex: Int): Ref = null

  override def getBlob(columnIndex: Int): Blob = ???

  override def getClob(columnIndex: Int): Clob = ???

  override def getArray(columnIndex: Int): sql.Array = ???

  override def getObject(columnLabel: String, map: util.Map[String, Class[_]]): AnyRef = null

  override def getRef(columnLabel: String): Ref = null

  override def getBlob(columnLabel: String): Blob = ???

  override def getClob(columnLabel: String): Clob = ???

  override def getArray(columnLabel: String): sql.Array = ???

  override def getDate(columnIndex: Int, cal: Calendar): Date = ???

  override def getDate(columnLabel: String, cal: Calendar): Date = ???

  override def getTime(columnIndex: Int, cal: Calendar): Time = ???

  override def getTime(columnLabel: String, cal: Calendar): Time = ???

  override def getTimestamp(columnIndex: Int, cal: Calendar): Timestamp = ???

  override def getTimestamp(columnLabel: String, cal: Calendar): Timestamp = ???

  override def getURL(columnIndex: Int): URL = ???

  override def getURL(columnLabel: String): URL = ???

  override def updateRef(columnIndex: Int, x: Ref): Unit = ()

  override def updateRef(columnLabel: String, x: Ref): Unit = ()

  override def updateBlob(columnIndex: Int, x: Blob): Unit = ()

  override def updateBlob(columnLabel: String, x: Blob): Unit = ()

  override def updateClob(columnIndex: Int, x: Clob): Unit = ()

  override def updateClob(columnLabel: String, x: Clob): Unit = ()

  override def updateArray(columnIndex: Int, x: sql.Array): Unit = ()

  override def updateArray(columnLabel: String, x: sql.Array): Unit = ()

  override def getRowId(columnIndex: Int): RowId = ???

  override def getRowId(columnLabel: String): RowId = ???

  override def updateRowId(columnIndex: Int, x: RowId): Unit = ()

  override def updateRowId(columnLabel: String, x: RowId): Unit = ()

  override def getHoldability: Int = 0

  override def isClosed: Boolean = false

  override def updateNString(columnIndex: Int, nString: String): Unit = ()

  override def updateNString(columnLabel: String, nString: String): Unit = ()

  override def updateNClob(columnIndex: Int, nClob: NClob): Unit = ()

  override def updateNClob(columnLabel: String, nClob: NClob): Unit = ()

  override def getNClob(columnIndex: Int): NClob = ???

  override def getNClob(columnLabel: String): NClob = ???

  override def getSQLXML(columnIndex: Int): SQLXML = ???

  override def getSQLXML(columnLabel: String): SQLXML = ???

  override def updateSQLXML(columnIndex: Int, xmlObject: SQLXML): Unit = ()

  override def updateSQLXML(columnLabel: String, xmlObject: SQLXML): Unit = ()

  override def getNString(columnIndex: Int): String = ???

  override def getNString(columnLabel: String): String = ???

  override def getNCharacterStream(columnIndex: Int): Reader = ???

  override def getNCharacterStream(columnLabel: String): Reader = ???

  override def updateNCharacterStream(columnIndex: Int, x: Reader, length: Long): Unit = ()

  override def updateNCharacterStream(columnLabel: String, reader: Reader, length: Long): Unit = ()

  override def updateAsciiStream(columnIndex: Int, x: InputStream, length: Long): Unit = ()

  override def updateBinaryStream(columnIndex: Int, x: InputStream, length: Long): Unit = ()

  override def updateCharacterStream(columnIndex: Int, x: Reader, length: Long): Unit = ()

  override def updateAsciiStream(columnLabel: String, x: InputStream, length: Long): Unit = ()

  override def updateBinaryStream(columnLabel: String, x: InputStream, length: Long): Unit = ()

  override def updateCharacterStream(columnLabel: String, reader: Reader, length: Long): Unit = ()

  override def updateBlob(columnIndex: Int, inputStream: InputStream, length: Long): Unit = ()

  override def updateBlob(columnLabel: String, inputStream: InputStream, length: Long): Unit = ()

  override def updateClob(columnIndex: Int, reader: Reader, length: Long): Unit = ()

  override def updateClob(columnLabel: String, reader: Reader, length: Long): Unit = ()

  override def updateNClob(columnIndex: Int, reader: Reader, length: Long): Unit = ()

  override def updateNClob(columnLabel: String, reader: Reader, length: Long): Unit = ()

  override def updateNCharacterStream(columnIndex: Int, x: Reader): Unit = ()

  override def updateNCharacterStream(columnLabel: String, reader: Reader): Unit = ()

  override def updateAsciiStream(columnIndex: Int, x: InputStream): Unit = ()

  override def updateBinaryStream(columnIndex: Int, x: InputStream): Unit = ()

  override def updateCharacterStream(columnIndex: Int, x: Reader): Unit = ()

  override def updateAsciiStream(columnLabel: String, x: InputStream): Unit = ()

  override def updateBinaryStream(columnLabel: String, x: InputStream): Unit = ()

  override def updateCharacterStream(columnLabel: String, reader: Reader): Unit = ()

  override def updateBlob(columnIndex: Int, inputStream: InputStream): Unit = ()

  override def updateBlob(columnLabel: String, inputStream: InputStream): Unit = ()

  override def updateClob(columnIndex: Int, reader: Reader): Unit = ()

  override def updateClob(columnLabel: String, reader: Reader): Unit = ()

  override def updateNClob(columnIndex: Int, reader: Reader): Unit = ()

  override def updateNClob(columnLabel: String, reader: Reader): Unit = ()

  override def getObject[T](columnIndex: Int, `type`: Class[T]): T = ???

  override def getObject[T](columnLabel: String, `type`: Class[T]): T = {
    val value: Any = currentData.getAs(columnLabel)

    val ldtCls = classOf[LocalDateTime]

    `type` match {
      case ldtCls =>
        // from timestamp => localdatetime
        value.asInstanceOf[Timestamp].toLocalDateTime.asInstanceOf[T]
      case _ => value.asInstanceOf[T]
    }
  }

  override def unwrap[T](iface: Class[T]): T = ???

  override def isWrapperFor(iface: Class[_]): Boolean = ???
}

// scalastyle:on
