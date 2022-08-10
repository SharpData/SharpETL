package com.github.sharpdata.sharpetl.spark.utils

import ETLSparkSession.sparkSession
import org.apache.spark.sql.functions._
import com.github.sharpdata.sharpetl.datasource.kafka.DFConversations._

object SparkCatalogUtil {
  def getPartitionColNames(dbName: String, tableName: String): Array[String] = {
    sparkSession.catalog.listColumns(dbName, tableName)
      .where(col("ispartition") === true)
      .select("name")
      .collect()
      .map(_.getAs[String]("name"))
  }

  def getNonePartitionColNames(dbName: String, tableName: String): Array[String] = {
    sparkSession.catalog.listColumns(dbName, tableName)
      .where(col("ispartition") === false)
      .select("name")
      .collect()
      .map(_.getAs[String]("name"))
  }

  def getAllColNames(dbName: String, tableName: String): Array[String] = {
    Array.concat(
      getNonePartitionColNames(dbName, tableName),
      getPartitionColNames(dbName, tableName)
    )
  }

  def getAllColNamesOfTempTable(tableName: String): Array[String] = {
    sparkSession.catalog.listColumns(tableName)
      .select("name")
      .collect()
      .map(_.getAs[String]("name"))
  }

  def isPartitionDataExists(tableName: String, partitionQuery: String): Boolean = {
    !sparkSession.sql(s"select * from $tableName where $partitionQuery limit 1").isEmpty
  }
}
