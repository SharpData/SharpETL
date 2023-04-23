package com.github.sharpdata.sharpetl.core.util

import org.flywaydb.core.Flyway

object FlywayUtil {
  def migrate(): Unit = {

    val flyway = if (ETLConfig.getProperty("flyway.url").toLowerCase().contains("jdbc:sqlserver:")) {
      // MS Sql Server
      Flyway
        .configure
        .schemas("sharp_etl")
        .defaultSchema("sharp_etl")
        .createSchemas(true)
        .locations("db/sqlserver/migration")
        .dataSource(
          ETLConfig.getProperty("flyway.url"),
          ETLConfig.getProperty("flyway.username"),
          ETLConfig.getProperty("flyway.password"))
        .load()
    } else if (ETLConfig.getProperty("flyway.url").toLowerCase().contains("jdbc:spark_sharp_etl:")) {
      Flyway
        .configure
        .locations("db/spark/migration")
        .defaultSchema("sharp_etl")
        .createSchemas(true)
        .baselineVersion("0")
        //.baselineOnMigrate(true)
        .dataSource(
          ETLConfig.getProperty("flyway.url"),
          ETLConfig.getProperty("flyway.username"),
          ETLConfig.getProperty("flyway.password"))
        .load()
    } else {
      // MySQL
      Flyway
        .configure
        .locations("db/mysql/migration")
        .dataSource(
          ETLConfig.getProperty("flyway.url"),
          ETLConfig.getProperty("flyway.username"),
          ETLConfig.getProperty("flyway.password"))
        .load()
    }
    flyway.migrate()
  }
}
