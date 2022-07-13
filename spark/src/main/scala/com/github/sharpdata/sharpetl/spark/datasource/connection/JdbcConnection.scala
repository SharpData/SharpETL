package com.github.sharpdata.sharpetl.spark.datasource.connection

import com.github.sharpdata.sharpetl.core.util.{ETLConfig, StringUtil}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

import java.sql.{Connection, DriverManager}

case class JdbcConnection(connectionName: String, jdbcType: String) {

  private val _prefix = StringUtil.getPrefix(connectionName)

  private lazy val defaultConfig: Map[String, String] = {
    Map(
      JDBCOptions.JDBC_URL -> ETLConfig.getProperty(s"${_prefix}${jdbcType}.url"),
      "user" -> ETLConfig.getProperty(s"${_prefix}${jdbcType}.user"),
      "password" -> ETLConfig.getProperty(s"${_prefix}${jdbcType}.password"),
      JDBCOptions.JDBC_DRIVER_CLASS -> ETLConfig.getProperty(s"${_prefix}${jdbcType}.driver"),
      // 每次获取数据的行数，在 jdbc 上设成较小值有利于性能优化，默认为 10
      JDBCOptions.JDBC_BATCH_FETCH_SIZE -> ETLConfig.getProperty(s"${_prefix}${jdbcType}.fetchsize")
    )
  }

  def getConnection(): Connection = {
    Class.forName(defaultConfig(JDBCOptions.JDBC_DRIVER_CLASS))
    DriverManager.getConnection(defaultConfig(JDBCOptions.JDBC_URL), defaultConfig("user"), defaultConfig("password"))
  }

  def buildConfig(options: Map[String, String]): Map[String, String] = {
    options ++ defaultConfig
  }

  def getDefaultConfig: Map[String, String] = {
    defaultConfig
  }
}
