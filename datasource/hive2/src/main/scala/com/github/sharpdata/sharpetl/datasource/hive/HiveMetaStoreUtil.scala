package com.github.sharpdata.sharpetl.datasource.hive

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.{StructField, StructType}

import scala.jdk.CollectionConverters._

object HiveMetaStoreUtil {
  private var isClosed: Boolean = _
  private var hiveMetaStoreClient: HiveMetaStoreClient = _

  private def createHiveMetaStoreClient(): Unit = {
    try {
      /**
       * took from <a href="https://issues.streamsets.com/browse/SDC-11231"/>
       */
      /**
       * previously (Hive 1.2 and 2.1) received as argument an [[org.apache.hadoop.hive.conf.HiveConf]] object
       */
      this.hiveMetaStoreClient = new HiveMetaStoreClient(new HiveConf)
      isClosed = false
    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw e
    }
  }

  def getHiveMetaStoreClient: HiveMetaStoreClient = {
    if (this.hiveMetaStoreClient == null) {
      createHiveMetaStoreClient()
    }
    if (isClosed) {
      this.hiveMetaStoreClient.reconnect()
    }
    this.hiveMetaStoreClient
  }

  def closeHiveMetaStoreClient(): Unit = {
    if (this.hiveMetaStoreClient != null && !isClosed) {
      this.hiveMetaStoreClient.close()
      isClosed = true
    }
  }

  def getHiveTableStructType(dbName: String, tableName: String): StructType = {
    fieldSchema2StructType(
      HiveMetaStoreUtil.getHiveTableAllCols(dbName, tableName)
    )
  }

  def fieldSchema2StructType(fieldSchemaArray: Array[FieldSchema]): StructType = {
    StructType(
      fieldSchemaArray.map(fieldSchema => StructField(
        fieldSchema.getName,
        CatalystSqlParser.parseDataType(fieldSchema.getType)
      ))
    )
  }

  def getHiveTablePartitionColNames(dbName: String, tableName: String): Array[String] = {
    getHiveTablePartitionCols(dbName, tableName).map(_.getName)
  }

  def getHiveTablePartitionCols(dbName: String, tableName: String): Array[FieldSchema] = {
    getHiveMetaStoreClient
      .getTable(dbName, tableName)
      .getPartitionKeys
      .asScala
      .toArray
  }

  def getHiveTableNonePartitionColNames(dbName: String, tableName: String): Array[String] = {
    getHiveTableNonePartitionCols(dbName, tableName).map(_.getName)
  }

  def getHiveTableNonePartitionCols(dbName: String, tableName: String): Array[FieldSchema] = {
    getHiveMetaStoreClient
      .getTable(dbName, tableName)
      .getSd
      .getCols
      .asScala
      .toArray
  }

  def getHiveTableAllColNames(dbName: String, tableName: String): Array[String] = {
    Array.concat(
      getHiveTableNonePartitionColNames(dbName, tableName),
      getHiveTablePartitionColNames(dbName, tableName)
    )
  }

  def getHiveTableAllCols(dbName: String, tableName: String): Array[FieldSchema] = {
    Array.concat(
      getHiveTableNonePartitionCols(dbName, tableName),
      getHiveTablePartitionCols(dbName, tableName)
    )
  }

}
