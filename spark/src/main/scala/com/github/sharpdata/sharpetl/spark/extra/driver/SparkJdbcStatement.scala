package com.github.sharpdata.sharpetl.spark.extra.driver

import com.github.sharpdata.sharpetl.spark.utils.ETLSparkSession.sparkSession

import java.sql.{Connection, ResultSet, SQLWarning, Statement}

class SparkJdbcStatement extends Statement {

  private var resultSet: SparkJdbcResultSet = null

  override def executeQuery(sql: String): ResultSet = {
    println(s"[DRIVER] exscuting sql $sql")
    new SparkJdbcResultSet(sparkSession.sql(sql), this)
  }

  override def executeUpdate(sql: String): Int = {
    println(s"[DRIVER] exscuting sql $sql")
    sparkSession.sql(sql)
    0
  }

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
    this.resultSet = new SparkJdbcResultSet(sparkSession.sql(sql), this)
    true
  }

  override def getResultSet: ResultSet = resultSet

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

  override def executeBatch(): Array[Int] = Array.empty

  override def getConnection: Connection = new SparkJdbcConnection()

  override def getMoreResults(current: Int): Boolean = !this.resultSet.alreadyTheLast()

  override def getGeneratedKeys: ResultSet = null

  override def executeUpdate(sql: String, autoGeneratedKeys: Int): Int = 0

  override def executeUpdate(sql: String, columnIndexes: Array[Int]): Int = 0

  override def executeUpdate(sql: String, columnNames: Array[String]): Int = 0

  override def execute(sql: String, autoGeneratedKeys: Int): Boolean = false

  override def execute(sql: String, columnIndexes: Array[Int]): Boolean = false

  override def execute(sql: String, columnNames: Array[String]): Boolean = false

  override def getResultSetHoldability: Int = 0

  override def isClosed: Boolean = false

  override def setPoolable(poolable: Boolean): Unit = ()

  override def isPoolable: Boolean = false

  override def closeOnCompletion(): Unit = ()

  override def isCloseOnCompletion: Boolean = false

  override def unwrap[T](iface: Class[T]): T = ???

  override def isWrapperFor(iface: Class[_]): Boolean = false
}
