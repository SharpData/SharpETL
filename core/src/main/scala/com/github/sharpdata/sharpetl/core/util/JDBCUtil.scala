package com.github.sharpdata.sharpetl.core.util

object JDBCUtil {
  lazy val dbType: String = {
    val jdbcUrl = ETLConfig.getProperty("flyway.url").toLowerCase()
    if (jdbcUrl.contains(":sqlserver:")) {
      Constants.ETLDatabaseType.MSSQL
    } else if (jdbcUrl.contains("jdbc:h2")) {
      Constants.ETLDatabaseType.H2
    } else {
      Constants.ETLDatabaseType.MYSQL
    }
  }
}

object JdbcDefaultOptions {
  val PARTITION_NUM = 8
  val BATCH_SIZE = 1024
}

