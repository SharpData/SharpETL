package com.github.sharpdata.sharpetl.spark.extra.driver

import com.github.sharpdata.sharpetl.core.util.DateUtil.L_YYYY_MM_DD_HH_MM_SS
import com.github.sharpdata.sharpetl.spark.utils.ETLSparkSession.sparkSession
import org.apache.commons.lang.StringEscapeUtils

import java.io.{InputStream, Reader}
import java.net.URL
import java.sql._
import java.time.LocalDateTime
import java.util.Calendar
import scala.collection.mutable

//noinspection ScalaStyle
class SparkJdbcPreparedStatement(val sql: String) extends PreparedStatement {
  private var resultSet: SparkJdbcResultSet = null

  private val parameterMetaData = mutable.Map[Int, Any]()

  def escape(value: String): String = value.replace("'", "")

  def buildSql: String = {
    parameterMetaData.toList.sortBy(_._1)
      .foldLeft(sql) {
        case (accSql, (_, value)) =>
          value match {
            case _: Int | _: Boolean => accSql.replaceFirst("\\?", value.toString)
            case _: String => accSql.replaceFirst("\\?", s"\'${escape(value.toString)}\'")
            case time: LocalDateTime => accSql.replaceFirst("\\?", s"\'${time.format(L_YYYY_MM_DD_HH_MM_SS)}\'")
            case _ => accSql.replaceFirst("\\?", s"\'${escape(value.toString)}\'")
          }
      }
  }

  override def executeQuery(): ResultSet = {
    println(s"[DRIVER] executing sql $buildSql")
    new SparkJdbcResultSet(sparkSession.sql(buildSql), this)
  }

  override def executeUpdate(): Int = {
    println(s"[DRIVER] executing sql $buildSql")
    sparkSession.sql(buildSql)
    0
  }

  override def setNull(parameterIndex: Int, sqlType: Int): Unit = parameterMetaData.put(parameterIndex, sqlType)

  override def setBoolean(parameterIndex: Int, x: Boolean): Unit = parameterMetaData.put(parameterIndex, x)

  override def setByte(parameterIndex: Int, x: Byte): Unit = parameterMetaData.put(parameterIndex, x)

  override def setShort(parameterIndex: Int, x: Short): Unit = parameterMetaData.put(parameterIndex, x)

  override def setInt(parameterIndex: Int, x: Int): Unit = parameterMetaData.put(parameterIndex, x)

  override def setLong(parameterIndex: Int, x: Long): Unit = parameterMetaData.put(parameterIndex, x)

  override def setFloat(parameterIndex: Int, x: Float): Unit = parameterMetaData.put(parameterIndex, x)

  override def setDouble(parameterIndex: Int, x: Double): Unit = parameterMetaData.put(parameterIndex, x)

  override def setBigDecimal(parameterIndex: Int, x: java.math.BigDecimal): Unit = parameterMetaData.put(parameterIndex, x)

  override def setString(parameterIndex: Int, x: String): Unit = parameterMetaData.put(parameterIndex, x)

  override def setBytes(parameterIndex: Int, x: scala.Array[Byte]): Unit = parameterMetaData.put(parameterIndex, x)

  override def setDate(parameterIndex: Int, x: Date): Unit = parameterMetaData.put(parameterIndex, x)

  override def setTime(parameterIndex: Int, x: Time): Unit = parameterMetaData.put(parameterIndex, x)

  override def setTimestamp(parameterIndex: Int, x: Timestamp): Unit = parameterMetaData.put(parameterIndex, x)

  override def setAsciiStream(parameterIndex: Int, x: InputStream, length: Int): Unit = parameterMetaData.put(parameterIndex, x)

  override def setUnicodeStream(parameterIndex: Int, x: InputStream, length: Int): Unit = parameterMetaData.put(parameterIndex, x)

  override def setBinaryStream(parameterIndex: Int, x: InputStream, length: Int): Unit = parameterMetaData.put(parameterIndex, x)

  override def clearParameters(): Unit = parameterMetaData.clear()

  override def setObject(parameterIndex: Int, x: Any, targetSqlType: Int): Unit = parameterMetaData.put(parameterIndex, x)

  override def setObject(parameterIndex: Int, x: Any): Unit = parameterMetaData.put(parameterIndex, x)

  override def execute(): Boolean = {
    executeUpdate()
    true
  }

  override def addBatch(): Unit = ()

  override def setCharacterStream(parameterIndex: Int, reader: Reader, length: Int): Unit = ()

  override def setRef(parameterIndex: Int, x: Ref): Unit = ()

  override def setBlob(parameterIndex: Int, x: Blob): Unit = ()

  override def setClob(parameterIndex: Int, x: Clob): Unit = ()

  override def setArray(parameterIndex: Int, x: java.sql.Array): Unit = ()

  override def getMetaData: ResultSetMetaData = ???

  override def setDate(parameterIndex: Int, x: Date, cal: Calendar): Unit = parameterMetaData.put(parameterIndex, x)

  override def setTime(parameterIndex: Int, x: Time, cal: Calendar): Unit = parameterMetaData.put(parameterIndex, x)

  override def setTimestamp(parameterIndex: Int, x: Timestamp, cal: Calendar): Unit = {
    parameterMetaData.put(parameterIndex, x)
  }

  override def setNull(parameterIndex: Int, sqlType: Int, typeName: String): Unit = parameterMetaData.put(parameterIndex, null)

  override def setURL(parameterIndex: Int, x: URL): Unit = parameterMetaData.put(parameterIndex, x)

  override def getParameterMetaData: ParameterMetaData = ??? //TODO "PreparedStatement.setTimestamp threw a NullPointerException if getParameterMetaData() was called before the statement was executed. This fix adds the missing null checks to getParameterMetaData() to avoid the exception."

  override def setRowId(parameterIndex: Int, x: RowId): Unit = ()

  override def setNString(parameterIndex: Int, value: String): Unit = ()

  override def setNCharacterStream(parameterIndex: Int, value: Reader, length: Long): Unit = ()

  override def setNClob(parameterIndex: Int, value: NClob): Unit = ()

  override def setClob(parameterIndex: Int, reader: Reader, length: Long): Unit = ()

  override def setBlob(parameterIndex: Int, inputStream: InputStream, length: Long): Unit = ()

  override def setNClob(parameterIndex: Int, reader: Reader, length: Long): Unit = ()

  override def setSQLXML(parameterIndex: Int, xmlObject: SQLXML): Unit = ()

  override def setObject(parameterIndex: Int, x: Any, targetSqlType: Int, scaleOrLength: Int): Unit = parameterMetaData.put(parameterIndex, x)

  override def setAsciiStream(parameterIndex: Int, x: InputStream, length: Long): Unit = ()

  override def setBinaryStream(parameterIndex: Int, x: InputStream, length: Long): Unit = ()

  override def setCharacterStream(parameterIndex: Int, reader: Reader, length: Long): Unit = ()

  override def setAsciiStream(parameterIndex: Int, x: InputStream): Unit = ()

  override def setBinaryStream(parameterIndex: Int, x: InputStream): Unit = ()

  override def setCharacterStream(parameterIndex: Int, reader: Reader): Unit = ()

  override def setNCharacterStream(parameterIndex: Int, value: Reader): Unit = ()

  override def setClob(parameterIndex: Int, reader: Reader): Unit = ()

  override def setBlob(parameterIndex: Int, inputStream: InputStream): Unit = ()

  override def setNClob(parameterIndex: Int, reader: Reader): Unit = ()

  override def executeQuery(sql: String): ResultSet = ???

  override def executeUpdate(sql: String): Int = 0

  override def close(): Unit = ()

  override def getMaxFieldSize: Int = 0

  override def setMaxFieldSize(max: Int): Unit = ()

  override def getMaxRows: Int = 0

  override def setMaxRows(max: Int): Unit = ()

  override def setEscapeProcessing(enable: Boolean): Unit = ()

  override def getQueryTimeout: Int = 0

  override def setQueryTimeout(seconds: Int): Unit = ()

  override def cancel(): Unit = ()

  override def getWarnings: SQLWarning = null

  override def clearWarnings(): Unit = ()

  override def setCursorName(name: String): Unit = ()

  override def execute(sql: String): Boolean = {
    this.resultSet = new SparkJdbcResultSet(sparkSession.sql(buildSql), this)
    true
  }

  override def getResultSet: ResultSet = new SparkJdbcResultSet(sparkSession.sql(buildSql), this)

  override def getUpdateCount: Int = -1

  override def getMoreResults: Boolean = !this.resultSet.alreadyTheLast()

  override def setFetchDirection(direction: Int): Unit = ()

  override def getFetchDirection: Int = 0

  override def setFetchSize(rows: Int): Unit = ()

  override def getFetchSize: Int = 0

  override def getResultSetConcurrency: Int = 0

  override def getResultSetType: Int = 0

  override def addBatch(sql: String): Unit = ()

  override def clearBatch(): Unit = ()

  override def executeBatch(): scala.Array[Int] = ???

  override def getConnection: Connection = ???

  override def getMoreResults(current: Int): Boolean = !this.resultSet.alreadyTheLast()

  override def getGeneratedKeys: ResultSet = ???

  override def executeUpdate(sql: String, autoGeneratedKeys: Int): Int = {
    sparkSession.sql(sql)
    0
  }

  override def executeUpdate(sql: String, columnIndexes: scala.Array[Int]): Int = {
    sparkSession.sql(sql)
    0
  }

  override def executeUpdate(sql: String, columnNames: scala.Array[String]): Int = {
    sparkSession.sql(sql)
    0
  }

  override def execute(sql: String, autoGeneratedKeys: Int): Boolean = {
    sparkSession.sql(sql)
    true
  }

  override def execute(sql: String, columnIndexes: scala.Array[Int]): Boolean = {
    sparkSession.sql(sql)
    true
  }

  override def execute(sql: String, columnNames: scala.Array[String]): Boolean = {
    sparkSession.sql(sql)
    true
  }

  override def getResultSetHoldability: Int = 0

  override def isClosed: Boolean = false

  override def setPoolable(poolable: Boolean): Unit = ()

  override def isPoolable: Boolean = false

  override def closeOnCompletion(): Unit = ()

  override def isCloseOnCompletion: Boolean = false

  override def unwrap[T](iface: Class[T]): T = ???

  override def isWrapperFor(iface: Class[_]): Boolean = false
}
