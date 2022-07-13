package com.github.sharpdata.sharpetl.spark.utils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.jdbc.{DriverRegistry, DriverWrapper, JDBCOptions}

import java.sql.{Connection, Driver, DriverManager}
import scala.jdk.CollectionConverters._

// scalastyle:off
object JdbcUtils extends Logging {
  /**
   * Returns a factory for creating connections to the given JDBC URL.
   *
   * @param options - JDBC options that contains url, table and other information.
   */
  def createConnectionFactory(options: JDBCOptions): () => Connection = {
    val driverClass: String = options.driverClass
    () => {
      DriverRegistry.register(driverClass)
      val driver: Driver = DriverManager.getDrivers.asScala.collectFirst {
        case d: DriverWrapper if d.wrapped.getClass.getCanonicalName == driverClass => d
        case d if d.getClass.getCanonicalName == driverClass => d
      }.getOrElse {
        throw new IllegalStateException(
          s"Did not find registered driver with class $driverClass")
      }
      driver.connect(options.url, options.asConnectionProperties)
    }
  }
}
// scalastyle:on
