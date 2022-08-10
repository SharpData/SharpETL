package com.github.sharpdata.sharpetl.datasource.kudu

import com.github.sharpdata.sharpetl.core.api.Variables
import com.github.sharpdata.sharpetl.core.datasource.{Sink, Source}
import com.github.sharpdata.sharpetl.core.annotation._
import com.github.sharpdata.sharpetl.core.datasource.config.DBDataSourceConfig
import com.github.sharpdata.sharpetl.core.repository.model.JobLog
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.core.util.Constants.{DataSourceType, WriteMode}
import com.github.sharpdata.sharpetl.core.util.{ETLConfig, ETLLogger}
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.{KuduContext, KuduWriteOptions}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

@source(types = Array("kudu", "impala_kudu"))
@sink(types = Array("kudu", "impala_kudu"))
class KuduDataSource(executionContext: SparkSession) extends Source[DataFrame, SparkSession] with Sink[DataFrame] {
  private val KUDU = "org.apache.kudu.spark.kudu"
  private val KUDU_MASTER = "kudu.master"
  private val KUDU_TABLE = "kudu.table"

  private val kuduMaster = ETLConfig.getProperty(KUDU_MASTER)

  lazy val kuduContext = new KuduContext(
    kuduMaster,
    executionContext.sparkContext
  )

  override def read(step: WorkflowStep, jobLog: JobLog, executionContext: SparkSession, variables: Variables): DataFrame = {
    val sourceConfig = step.getSourceConfig[DBDataSourceConfig]
    val df = executionContext
      .read
      .options(buildKuduLoadOptions(sourceConfig))
      .format(KUDU)
      .load
    val selectSql = step.getSql
    if (selectSql != null) {
      val tableName = sourceConfig.getTableName
      df.createOrReplaceTempView(tableName)
      ETLLogger.info(s"""[step${step.getStep}] Select Sql: \n$selectSql""")
      executionContext.sql(selectSql)
    } else {
      df
    }

  }

  override def write(df: DataFrame, step: WorkflowStep, variables: Variables): Unit = ???


  def tableExists(tableName: String): Boolean = {
    kuduContext.tableExists(tableName)
  }

  def createTable(
                   tableName: String,
                   schema: StructType,
                   keys: Seq[String],
                   options: CreateTableOptions): Unit = {
    if (!tableExists(tableName)) {
      kuduContext.createTable(
        tableName,
        schema,
        keys,
        options
      )
    }
  }

  def deleteTable(tableName: String): Unit = {
    if (tableExists(tableName)) {
      kuduContext.deleteTable(tableName)
    }
  }


  def buildKuduLoadOptions(sourceConfig: DBDataSourceConfig): Map[String, String] = {
    val dataSourceType = sourceConfig.getDataSourceType
    val kuduTable = dataSourceType.toLowerCase match {
      case DataSourceType.IMPALA_KUDU =>
        s"${ETLConfig.getProperty("kudu.table.prefix")}${sourceConfig.getDbName}.${sourceConfig.getTableName}"
      case DataSourceType.KUDU =>
        sourceConfig.getTableName
    }
    Map(
      KUDU_MASTER -> kuduMaster,
      KUDU_TABLE -> kuduTable
    ).++(sourceConfig.getOptions)
  }

  def save(
            df: DataFrame,
            dbName: String,
            kuduTable: String,
            writeMode: String
          ): Unit = {
    save(df, dbName, kuduTable, writeMode, new KuduWriteOptions())
  }

  def save(
            df: DataFrame,
            dbName: String,
            kuduTable: String,
            writeMode: String,
            kuduWriteOptions: KuduWriteOptions
          ): Unit = {
    val impalaKuduTable = s"${ETLConfig.getProperty("kudu.table.prefix")}$dbName.$kuduTable"
    save(df, impalaKuduTable, writeMode, kuduWriteOptions)
  }

  def save(
            df: DataFrame,
            kuduTable: String,
            writeMode: String
          ): Unit = {
    save(df, kuduTable, writeMode, new KuduWriteOptions())
  }

  def save(
            df: DataFrame, kuduTable: String, writeMode: String,
            kuduWriteOptions: KuduWriteOptions): Unit = {
    writeMode.toLowerCase match {
      case WriteMode.APPEND =>
        save(df, kuduTable)
      case WriteMode.UPSERT =>
        upsert(df, kuduTable, kuduWriteOptions)
      case WriteMode.DELETE =>
        delete(df, kuduTable, kuduWriteOptions)
    }
  }

  def save(
            df: DataFrame,
            kuduTable: String,
            kuduWriteOptions: KuduWriteOptions = new KuduWriteOptions): Unit = {
    kuduContext.insertRows(
      df,
      kuduTable,
      kuduWriteOptions
    )
  }

  def upsert(
              df: DataFrame,
              kuduTable: String,
              kuduWriteOptions: KuduWriteOptions = new KuduWriteOptions): Unit = {
    kuduContext.upsertRows(
      df,
      kuduTable,
      kuduWriteOptions
    )
  }

  def delete(
              df: DataFrame,
              kuduTable: String,
              kuduWriteOptions: KuduWriteOptions = new KuduWriteOptions): Unit = {
    kuduContext.deleteRows(
      df,
      kuduTable,
      kuduWriteOptions
    )
  }

}
