package com.github.sharpdata.sharpetl.spark.end2end.mysql

import com.github.sharpdata.sharpetl.spark.end2end.ETLSuit
import org.apache.spark.sql.DataFrame

import java.sql.{DriverManager, SQLException}

trait MysqlSuit extends ETLSuit {
  var dataPort: Int = 2334

  def writeDataToSource(sampleDataDf: DataFrame, tableName: String): Unit = {
    sampleDataDf.write
      .format("jdbc")
      .option("url", s"jdbc:mysql://localhost:$dataPort/$sourceDbName")
      .option("dbtable", tableName)
      .option("user", "admin")
      .option("password", "admin")
      .mode("append")
      .save()
  }

  def readFromSource(targetTable: String): DataFrame = {
    spark.read
      .format("jdbc")
      .option("url", s"jdbc:mysql://localhost:$dataPort/int_test")
      .option("dbtable", targetTable)
      .option("user", "admin")
      .option("password", "admin")
      .load()
      .drop("job_id", "job_time")
  }

  def executeInSource(sql: String, dbName: String): Boolean = {
    val url = s"jdbc:mysql://localhost:$dataPort/$dbName"
    val connection = DriverManager.getConnection(url, "admin", "admin")
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

  def execute(sql: String): Boolean = {
    executeInSource(sql, targetDbName)
  }
}