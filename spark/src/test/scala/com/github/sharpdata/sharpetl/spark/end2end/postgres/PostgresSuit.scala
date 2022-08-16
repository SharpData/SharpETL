package com.github.sharpdata.sharpetl.spark.end2end.postgres

import com.github.sharpdata.sharpetl.spark.end2end.ETLSuit
import org.apache.spark.sql.DataFrame

import java.sql.{DriverManager, SQLException}

trait PostgresEtlSuit extends ETLSuit {

  def readTable(dbName: String, port: Int, tableName: String): DataFrame = {
    spark.read
      .format("jdbc")
      .option("url", s"jdbc:postgresql://localhost:$port/$dbName")
      .option("dbtable", tableName)
      .option("user", "postgres")
      .option("password", "postgres")
      .load()
  }

  def execute(sql: String, dbName: String, port: Int): Boolean = {
    val url = s"jdbc:postgresql://localhost:$port/$dbName"
    val connection = DriverManager.getConnection(url, "postgres", "postgres")
    val statement = connection.createStatement()
    try {
      statement.execute(sql)
    } catch {
      case ex: SQLException => throw new RuntimeException(ex)
    } finally {
      if (connection != null) connection.close()
      if (statement != null) statement.close()
    }
  }
}
