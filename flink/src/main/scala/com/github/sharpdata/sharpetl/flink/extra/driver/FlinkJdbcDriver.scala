package com.github.sharpdata.sharpetl.flink.extra.driver

import java.sql.{Connection, Driver, DriverManager, DriverPropertyInfo, SQLException}
import java.util.Properties
import java.util.logging.Logger

// scalastyle:off
class FlinkJdbcDriver extends Driver {

  private var registered = false

  private def load() = {
    try if (!registered) {
      registered = true
      DriverManager.registerDriver(FlinkJdbcDriver.INSTANCE)
    }
    catch {
      case e: SQLException =>
        e.printStackTrace()
    }
    FlinkJdbcDriver.INSTANCE
  }

  override def connect(url: String, info: Properties): Connection = new FlinkJdbcConnection()

  override def acceptsURL(url: String): Boolean = if (url.contains("flink")) true else false

  override def getPropertyInfo(url: String, info: Properties): Array[DriverPropertyInfo] = Array.empty

  override def getMajorVersion: Int = 0

  override def getMinorVersion: Int = 0

  override def jdbcCompliant(): Boolean = false

  override def getParentLogger: Logger = null
}

object FlinkJdbcDriver {
  val INSTANCE = new FlinkJdbcDriver()
  INSTANCE.load()
}

// scalastyle:on