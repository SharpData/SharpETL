package com.github.sharpdata.sharpetl.spark.transformation

import com.github.sharpdata.sharpetl.core.util.{ETLLogger, HDFSUtil}
import com.github.sharpdata.sharpetl.spark.utils.ETLSparkSession
import org.apache.spark.sql

import scala.jdk.CollectionConverters._

// $COVERAGE-OFF$

case class PartitionField(key: String, value: String)

/*
  Example:
  - databaseName: developer
  - tableNamePrefix: pre_ods_
  - partition: year=2021,month=11,day=1,hour=10
 */

object DropExternalTableTransformer extends Transformer {
  override def transform(args: Map[String, String]): sql.DataFrame = {
    val tablePathPrefix = args("tablePathPrefix")
    val databaseName = args("databaseName")
    val tableNamePrefix = args("tableNamePrefix")
    val partitionParameter = args("partition")

    ETLSparkSession.getHiveSparkSession().sql(s"use ${databaseName}")
    val tables: Array[String] = ETLSparkSession.getHiveSparkSession().sql(s"show tables like '${tableNamePrefix}*'")
      .toLocalIterator()
      .asScala
      .map(row => {
        val value = row.toSeq.mkString(",")
        val seq = value.substring(1, value.length - 1).split(",")
        seq(1).substring(tableNamePrefix.length)
      })
      .toArray

    val partitionFields = extractPartitionField(partitionParameter)
    val databaseDirectory = s"/warehouse/tablespace/external/hive/${databaseName}.db"
    val tableDirectorys = HDFSUtil.listFileUrl(databaseDirectory, tablePathPrefix)

    tables.foreach(tableName => {
      val sql = buildDropPartitionSQL(databaseName, tableNamePrefix, tableName, partitionFields)
      ETLSparkSession.getHiveSparkSession().sql(sql)
      ETLLogger.info(s"Hive partition ${partitionFields.mkString("Array(", ", ", ")")} in table ${tableName} has been deleted.")
    })

    tableDirectorys.foreach(tableDirectory => {
      val partitionDirectory = buildHDFSDirectory(tableDirectory, partitionFields)
      if (HDFSUtil.exists(partitionDirectory)) {
        HDFSUtil.delete(partitionDirectory)
        ETLLogger.info(s"HDFS partition ${partitionFields.mkString("Array(", ", ", ")")} in directory ${tableDirectory} has been deleted.")
      }
    })

    ETLSparkSession.sparkSession.emptyDataFrame
  }

  def buildDropPartitionSQL(databaseName: String, tableNamePrefix: String, tableName: String, partitionFields: Seq[PartitionField]): String = {
    val partitionArguments = partitionFields.map(partitionField => s"${partitionField.key} = ${partitionField.value}").mkString(", ")
    val sql = s"ALTER TABLE `${databaseName}`.`${tableNamePrefix}${tableName}` DROP IF EXISTS PARTITION(${partitionArguments})"
    sql
  }

  def buildHDFSDirectory(tableDirectory: String, partitionFields: Seq[PartitionField]): String = {

    val partitionPath = partitionFields.map(partitionField => s"${partitionField.key}=${partitionField.value}").mkString("/")
    val partitionDirectory = s"${tableDirectory}/${partitionPath}"

    partitionDirectory
  }

  def extractPartitionField(argument: String): Array[PartitionField] = argument.split(",")
    .map(field => {
      val keyValue = field.split("=")
      PartitionField(keyValue(0), keyValue(1))
    })
}
// $COVERAGE-ON$
