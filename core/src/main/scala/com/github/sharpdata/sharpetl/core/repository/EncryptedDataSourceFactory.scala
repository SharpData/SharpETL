package com.github.sharpdata.sharpetl.core.repository

import com.github.sharpdata.sharpetl.core.util.ETLConfig
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.apache.ibatis.datasource.DataSourceFactory

import java.util.Properties
import javax.sql.DataSource

case object HikariDataSource {
  private val hikariConfig = new HikariConfig()

  lazy val hikariDataSource = new HikariDataSource(hikariConfig)

  def setProperties(properties: Properties): Unit = {
    hikariConfig.setDriverClassName(ETLConfig.getProperty("flyway.driver"))
    hikariConfig.setJdbcUrl(ETLConfig.getProperty("flyway.url"))
    hikariConfig.setUsername(ETLConfig.getProperty("flyway.username"))
    hikariConfig.setPassword(ETLConfig.getProperty("flyway.password"))
    // 1800000 (30 minutes)
    // hikariConfig.setKeepaliveTime(ETLConfig.getProperty("flyway.keepalivetime", "1800000").toLong)
    // A value of 0 indicates no maximum lifetime (infinite lifetime)
    hikariConfig.setMaxLifetime(ETLConfig.getProperty("flyway.maxlifetime", "0").toLong)
    hikariConfig.setMaximumPoolSize(ETLConfig.getProperty("flyway.maxpoolsize", "1").toInt)
  }
}

case class EncryptedDataSourceFactory() extends DataSourceFactory {
  override def setProperties(properties: Properties): Unit = {
    HikariDataSource.setProperties(properties)
  }

  override def getDataSource: DataSource = {
    HikariDataSource.hikariDataSource
  }
}
