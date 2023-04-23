package com.github.sharpdata.sharpetl.spark.extra.driver

import com.github.sharpdata.sharpetl.spark.extra.driver.SparkJdbcDriver.INSTANCE

import java.sql.{Connection, Driver, DriverPropertyInfo}
import java.util.Properties
import java.util.logging.Logger
import java.sql.DriverManager
import java.sql.SQLException


class SparkJdbcDriver extends Driver {

  private var registered = false

  private def load() = {
    try if (!registered) {
      registered = true
      DriverManager.registerDriver(INSTANCE)
    }
    catch {
      case e: SQLException =>
        e.printStackTrace()
    }
    INSTANCE
  }

  override def connect(url: String, info: Properties): Connection = new SparkJdbcConnection()

  override def acceptsURL(url: String): Boolean = if (url.contains("spark")) true else false

  override def getPropertyInfo(url: String, info: Properties): Array[DriverPropertyInfo] = Array.empty

  override def getMajorVersion: Int = 0

  override def getMinorVersion: Int = 0

  override def jdbcCompliant(): Boolean = false

  override def getParentLogger: Logger = null
}

object SparkJdbcDriver {
  val INSTANCE = new SparkJdbcDriver()
  INSTANCE.load()
}
