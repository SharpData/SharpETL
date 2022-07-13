package com.github.sharpdata.sharpetl.spark.datasource

import com.github.sharpdata.sharpetl.core.api.Variables
import com.github.sharpdata.sharpetl.core.datasource.{Sink, Source}
import com.google.common.base.Strings.isNullOrEmpty
import com.github.sharpdata.sharpetl.core.datasource.config.DBDataSourceConfig
import com.github.sharpdata.sharpetl.core.exception.Exception.InvalidSqlException
import com.github.sharpdata.sharpetl.core.repository.model.JobLog
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.core.util.Constants.WriteMode
import com.github.sharpdata.sharpetl.core.util.Constants.WriteMode.MERGE_WRITE
import com.github.sharpdata.sharpetl.core.util.StringUtil.{assertNotEmpty, uuid}
import com.github.sharpdata.sharpetl.core.util.{ETLLogger, StringUtil}
import com.github.sharpdata.sharpetl.core.annotation._
import com.github.sharpdata.sharpetl.spark.utils.ETLSparkSession.{autoPurgeHiveTable, sparkSession}
import com.github.sharpdata.sharpetl.datasource.kafka.DFConversations._
import com.github.sharpdata.sharpetl.spark.utils.SparkCatalogUtil
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.jdk.CollectionConverters._

@source(types = Array("hive"))
@sink(types = Array("hive"))
class HiveDataSource extends Sink[DataFrame] with Source[DataFrame, SparkSession] {

  override def sink(df: DataFrame, step: WorkflowStep, variables: Variables): Unit = {
    save(df, step.target.asInstanceOf[DBDataSourceConfig].getTableName, step, variables)
  }

  override def read(step: WorkflowStep, jobLog: JobLog, executionContext: SparkSession, variables: Variables): DataFrame = {
    load(executionContext, step.getSql)
  }

  def load(spark: SparkSession, selectSql: String): DataFrame = {
    // work-around for https://issues.apache.org/jira/browse/SPARK-38173
    // Fixed in spark 3.3.0 in https://github.com/apache/spark/commit/1ef5638177dcf06ebca4e9b0bc88401e0fce2ae8
    if (selectSql.contains("+.+")) {
      sparkSession.sql("SET spark.sql.parser.quotedRegexColumnNames=true")
      val df = spark.sql(selectSql)
      sparkSession.sql("SET spark.sql.parser.quotedRegexColumnNames=false")
      df
    } else {
      spark.sql(selectSql)
    }
  }

  def save(df: DataFrame, targetTable: String, step: WorkflowStep, variables: Variables): Unit = {
    val writeMode: String = step.writeMode
    val dbName = step.target.asInstanceOf[DBDataSourceConfig].dbName
    val resultTempTable = s"$dbName.$uuid"
    /*if (step.incrementalType == DIFF) {
      //this column MUST be partition column with name configured by [[com.github.sharpdata.sharpetl.core.util.ETLConfig.partitionColumn]]
      df.withColumn(partitionColumn, lit(variables(partitionColumn)))
    }*/
    if (!df.isEmpty) {
      try {
        df.write.mode(WriteMode.OVER_WRITE).saveAsTable(resultTempTable)
        ETLLogger.info(s"Saved data to temp table $resultTempTable")
        val insertSql = buildInsertSql(
          resultTempTable,
          df.schema.fieldNames.map(_.toLowerCase),
          dbName,
          targetTable,
          writeMode,
          step
        )
        ETLLogger.info(s"""[$targetTable] Insert Sql: $insertSql""")
        sparkSession.sql(insertSql)
      } finally {
        autoPurgeHiveTable(resultTempTable)
        sparkSession.sql(s"drop table if exists $resultTempTable")
      }
    } else {
      ETLLogger.error(s"Source is empty, nothing need to be written into target table $targetTable")
    }
  }

  private def buildInsertSql(sourceTable: String,
                             sourceSchema: Array[String],
                             targetDb: String,
                             targetTable: String,
                             writeMode: String,
                             step: WorkflowStep): String = {
    assertNotEmpty(targetDb, "targetDb")
    assertNotEmpty(targetTable, "targetTable")
    val prefix = if (isNullOrEmpty(targetDb)) StringUtil.EMPTY else s"$targetDb."
    val fullTablePath = s"$prefix$targetTable"
    autoPurgeHiveTable(fullTablePath)
    val targetPartitionColNames = SparkCatalogUtil.getPartitionColNames(targetDb, targetTable)
    val targetNonePartitionColNames = SparkCatalogUtil.getNonePartitionColNames(targetDb, targetTable)
    val targetAllColNames = Array.concat(targetNonePartitionColNames, targetPartitionColNames)
    val partitionClause = if (targetPartitionColNames.nonEmpty) {
      s"partition ${targetPartitionColNames.mkString("(", ", ", ")")}"
    } else {
      ""
    }
    verifySchema(sourceSchema, targetAllColNames)

    val selectClause = if (step.isUseTargetSchema.equals(true.toString)) {
      buildInsertSqlByTargetSchema(sourceSchema, targetAllColNames)
    } else {
      buildInsertSqlByComparingWithTargetSchema(sourceSchema, targetAllColNames)
    }

    val saveMode = writeMode match {
      case WriteMode.OVER_WRITE | MERGE_WRITE =>
        "overwrite"
      case WriteMode.APPEND =>
        "into"
    }

    s"""
       |insert $saveMode table $fullTablePath $partitionClause
       |select * from (
       |select $selectClause from $sourceTable
       |${if (writeMode == MERGE_WRITE) selfUnionClause(sourceTable, fullTablePath, targetPartitionColNames, writeMode, step) else StringUtil.EMPTY} )
       |distribute by ${(targetPartitionColNames ++ List("rand()")).mkString(",")}
       |""".stripMargin
  }

  private def verifySchema(sourceSchema: Array[String], targetAllColNames: Array[String]): Unit = {
    val sourceSet = sourceSchema.map(_.toLowerCase()).toSet
    val targetSet = targetAllColNames.map(_.toLowerCase()).toSet
    if (sourceSet.diff(targetSet).nonEmpty && targetSet.diff(sourceSet).nonEmpty) {
      ETLLogger.warn(
        s"""
           |sourceSchema is not the same with targetSchema.
           |
           |sourceSchema - targetSchema: ${sourceSet.diff(targetSet)}
           |targetSchema - sourceSchema: ${targetSet.diff(sourceSet)}
           |""".stripMargin)
    }
  }

  def buildInsertSqlByComparingWithTargetSchema(sourceSchema: Array[String], targetSchema: Array[String]): String = {
    targetSchema.map(targetColumn => {
      if (sourceSchema.contains(targetColumn)) {
        s"`$targetColumn`"
      } else {
        s"null as `$targetColumn`"
      }
    }).mkString(",\n       ")
  }

  /**
   * 是否默认使用target schema，仅在target column size > source column size 的时候才使用null填充超出部分，否则：
   * 只要target column不在source schema里面，就用as把相同index的source column转成对应index的target column
   */
  def buildInsertSqlByTargetSchema(sourceSchema: Array[String], targetSchema: Array[String]): String = {
    targetSchema.indices.map(idx => {
      if (idx >= sourceSchema.length) {
        s"null as `${targetSchema(idx)}`"
      } else {
        if (sourceSchema.contains(targetSchema(idx))) {
          s"`${targetSchema(idx)}` as `${targetSchema(idx)}`"
        } else {
          s"`${sourceSchema(idx)}` as `${targetSchema(idx)}`"
        }
      }
    }).mkString(",\n       ")
  }

  def selfUnionClause(sourceTable: String, fullTablePath: String, targetPartitionColNames: Array[String],
                      writeMode: String, step: WorkflowStep): String = {
    // idempotent support for [[MERGE_WRITE]] mode:
    // not select the (physical or logic) partition from target table, so we could safely overwrite the data of that partition
    val idempotentQueryClause = writeMode match {
      case MERGE_WRITE =>
        if (step.sql.toLowerCase.contains("where")) {
          s"""!(${step.sql.toLowerCase.split("where").reverse.head.trim.stripSuffix(";")})"""
        } else {
          throw InvalidSqlException("Where clause must exist in sql when using `mergewrite` mode!")
        }
      case _ =>
        StringUtil.EMPTY
    }
    val partitionWhereClause =
      sparkSession.sql(s"select ${targetPartitionColNames.mkString("distinct(", ", ", ")")} from $sourceTable")
        .toLocalIterator()
        .asScala
        .map(row => {
          val value = row.toSeq.mkString(",")
          val seq = value.substring(1, value.length - 1).split(",")
          targetPartitionColNames.zipWithIndex.map {
            case (col, idx) => s"$col = ${seq(idx)}"
          }.mkString("(", " and ", ")")
        })
        .mkString("(", " or ", ")")
    s"""union all
       |select * from $fullTablePath where $partitionWhereClause and $idempotentQueryClause""".stripMargin
  }
}

@source(types = Array("temp"))
@sink(types = Array("temp"))
class TempDataSource extends HiveDataSource {
  override def sink(df: DataFrame, step: WorkflowStep, variables: Variables): Unit = {
    df.createOrReplaceTempView(step.target.asInstanceOf[DBDataSourceConfig].getTableName)
  }
}
