package com.github.sharpdata.sharpetl.spark.datasource

import com.github.sharpdata.sharpetl.core.api.Variables
import com.github.sharpdata.sharpetl.core.datasource.config.DBDataSourceConfig
import com.github.sharpdata.sharpetl.core.repository.model.JobLog
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.core.annotation._
import com.github.sharpdata.sharpetl.core.datasource.{Sink, Source}
import com.github.sharpdata.sharpetl.core.util.Constants.WriteMode
import com.github.sharpdata.sharpetl.core.util.Constants.WriteMode.MERGE_WRITE
import com.github.sharpdata.sharpetl.datasource.kafka.DFConversations._
import com.github.sharpdata.sharpetl.core.util.ETLLogger
import com.github.sharpdata.sharpetl.core.util.StringUtil.uuid
import com.github.sharpdata.sharpetl.spark.utils.ETLSparkSession.sparkSession
import org.apache.spark.sql.{DataFrame, SparkSession}

@source(types = Array("delta_lake"))
@sink(types = Array("delta_lake"))
class DeltaLakeDataSource extends Source[DataFrame, SparkSession] with Sink[DataFrame] {

  override def read(step: WorkflowStep, jobLog: JobLog, executionContext: SparkSession, variables: Variables): DataFrame = {
    executionContext.sql(step.getSql)
  }

  override def write(df: DataFrame, step: WorkflowStep, variables: Variables): Unit = {
    save(df, step)
  }

  def save(df: DataFrame, step: WorkflowStep): Unit = {
    val writeMode: String = step.writeMode
    val config = step.target.asInstanceOf[DBDataSourceConfig]
    val dbName = config.dbName
    val resultTempTable = s"$uuid"
    val tableName = config.tableName
    if (!df.isEmpty) {
      try {
        if (writeMode == WriteMode.EXECUTE) {
          sparkSession.sql(step.sql)
        } else {
          df.createTempView(resultTempTable)
          ETLLogger.info(s"Saved data to temp table $resultTempTable")

          val saveMode = writeMode match {
            case WriteMode.OVER_WRITE | MERGE_WRITE =>
              "overwrite"
            case WriteMode.APPEND =>
              "into"
          }
          val insertSql =
            s"""
               |insert $saveMode table $dbName.$tableName
               |select * from $resultTempTable
               |""".stripMargin
          ETLLogger.info(s"""[$tableName] Insert Sql: $insertSql""")
          sparkSession.sql(insertSql)
        }
      } finally {
        sparkSession.sql(s"drop table if exists $resultTempTable")
      }
    } else {
      ETLLogger.error(s"Source is empty, nothing need to be written into target table: $tableName")
    }
  }

  /*private def write(df: DataFrame, step: WorkflowStep, targetPath: String) = {
    df
      .write
      .format("delta")
      .mode(step.getWriteMode)
      .save(targetPath)
  }

  private def writeByPartition(df: DataFrame, step: WorkflowStep, targetPath: String, partitionColumn: String) = {
    df
      .write
      .format("delta")
      .mode(step.getWriteMode)
      .partitionBy(partitionColumn)
      .save(targetPath)
  }

  def load(spark: SparkSession,
           step: WorkflowStep): DataFrame = {
    spark
      .read
      .format("delta")
      .load(s"$deltaLakeBasePath/${step.source.asInstanceOf[DBDataSourceConfig].tableName}")
  }*/
}
