package com.github.sharpdata.sharpetl.spark.datasource.jdbc

import com.github.sharpdata.sharpetl.core.api.Variables
import com.github.sharpdata.sharpetl.core.datasource.{Sink, Source}
import com.google.common.base.Strings.isNullOrEmpty
import com.github.sharpdata.sharpetl.core.datasource.config.DBDataSourceConfig
import com.github.sharpdata.sharpetl.core.repository.model.JobLog
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.core.util.Constants.JdbcDataType._
import com.github.sharpdata.sharpetl.core.util.Constants.WriteMode.{APPEND, DELETE, EXECUTE, OVER_WRITE, UPSERT}
import com.github.sharpdata.sharpetl.core.util.{ETLLogger, JdbcDefaultOptions}
import com.github.sharpdata.sharpetl.spark.datasoruce.jdbc.AbstractJdbcDataSource.addTaskCompletionListener
import com.github.sharpdata.sharpetl.spark.datasource.connection.JdbcConnection
import com.github.sharpdata.sharpetl.spark.utils.JdbcUtils.createConnectionFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.util.LongAccumulator

import java.sql.{Connection, PreparedStatement, ResultSet}
import scala.util.control.NonFatal

abstract class AbstractJdbcDataSource(jdbcType: String) extends Serializable with Source[DataFrame, SparkSession] with Sink[DataFrame] {

  override def read(step: WorkflowStep, jobLog: JobLog, executionContext: SparkSession, variables: Variables): DataFrame = {
    load(executionContext, step, variables)
  }

  override def write(df: DataFrame, step: WorkflowStep, variables: Variables): Unit = {
    save(df, step)
  }

  def buildSelectSql(selectSql: String): String = s"($selectSql)"

  def load(sparkSession: SparkSession, step: WorkflowStep, variables: Variables): DataFrame = {
    val sourceConfig = step.getSourceConfig[DBDataSourceConfig]
    sourceConfig.setLowerBound(variables.getOrElse("${lowerBound}", sourceConfig.getLowerBound))
    sourceConfig.setUpperBound(variables.getOrElse("${upperBound}", sourceConfig.getUpperBound))
    sourceConfig.setNumPartitions(variables.getOrElse("${numPartitions}", sourceConfig.getNumPartitions))

    val connectionName = Option(sourceConfig.getConnectionName).getOrElse(sourceConfig.getDbName)
    assert(!isNullOrEmpty(connectionName))
    val options = Map(
      JDBCOptions.JDBC_TABLE_NAME -> buildSelectSql(step.getSql),
      JDBCOptions.JDBC_NUM_PARTITIONS -> sourceConfig.getNumPartitions,
      JDBCOptions.JDBC_PARTITION_COLUMN -> sourceConfig.getPartitionColumn,
      JDBCOptions.JDBC_UPPER_BOUND -> sourceConfig.getUpperBound,
      JDBCOptions.JDBC_LOWER_BOUND -> sourceConfig.getLowerBound
    ).++(step.source.getOptions)
      .filter(_._2 != null)

    load(sparkSession, options, connectionName)
  }

  def load(sparkSession: SparkSession, options: Map[String, String], connectionName: String): DataFrame = {
    val jdbcConfig = buildJdbcConfig(connectionName, jdbcType, options)
    if (jdbcConfig("url") == null) {
      throw new IllegalArgumentException(s"url for JDBC connection is missing from application.properties for prefix: $connectionName")
    }
    if (jdbcConfig("driver") == null) {
      throw new IllegalArgumentException(s"driver for JDBC connection is missing from application.properties for prefix: $connectionName")
    }
    val config = jdbcConfig.filterNot(it => isNullOrEmpty(it._2))
    load(sparkSession, config)
  }

  private def load(sparkSession: SparkSession,
                   jdbcOptions: Map[String, String]): DataFrame = {
    sparkSession
      .read
      .format("jdbc")
      .options(jdbcOptions)
      .load().cache()
  }

  // scalastyle:off
  def loadPartition(schema: StructType,
                    options: JDBCOptions,
                    getConnection: () => Connection,
                    sql: String): Iterator[Row] = {
    var closed = false
    var rs: ResultSet = null
    var stmt: PreparedStatement = null
    var conn: Connection = null

    def close(): Unit = {
      if (closed) return
      try {
        if (null != rs) {
          rs.close()
        }
      } catch {
        case e: Exception => ETLLogger.error("Exception closing resultset", e)
      }
      try {
        if (null != stmt) {
          stmt.close()
        }
      } catch {
        case e: Exception => ETLLogger.error("Exception closing statement", e)
      }
      try {
        if (null != conn) {
          if (!conn.isClosed && !conn.getAutoCommit) {
            try {
              conn.commit()
            } catch {
              case NonFatal(e) => ETLLogger.error("Exception committing transaction", e)
            }
          }
          conn.close()
        }
        ETLLogger.info("closed connection")
      } catch {
        case e: Exception => ETLLogger.error("Exception closing connection", e)
      }
      closed = true
    }

    addTaskCompletionListener(close)
    conn = getConnection()

    stmt = conn.prepareStatement(sql,
      ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    stmt.setFetchSize(options.fetchSize)
    rs = stmt.executeQuery()
    val result = JdbcUtils.resultSetToRows(rs, schema)
    result
  }

  // scalastyle:on

  protected val dataTypeMapping: Map[String, DataType] = Map(
    VARCHAR -> StringType,
    CHAR -> StringType,
    DECIMAL -> DoubleType,
    TIMESTAMP -> TimestampType,
    BIGINT -> LongType,
    DOUBLE -> DoubleType,
    NUMERIC -> DoubleType,
    BPCHAR -> StringType
  )

  def buildJdbcConfig(connectionName: String, jdbcType: String, options: Map[String, String]): Map[String, String] = {
    JdbcConnection(connectionName, jdbcType).buildConfig(options)
  }

  def save(dataFrame: DataFrame, step: WorkflowStep): Unit = {
    val targetConfig = step.getTargetConfig[DBDataSourceConfig]
    val targetTableName = targetConfig.getTableName
    val targetDBName = targetConfig.getDbName
    val (primaryCols, notPrimaryCols) = getCols(targetDBName, targetTableName)

    val options = Map(
      JDBCOptions.JDBC_NUM_PARTITIONS -> Option(targetConfig.getNumPartitions).getOrElse(JdbcDefaultOptions.PARTITION_NUM.toString),
      JDBCOptions.JDBC_BATCH_INSERT_SIZE -> Option(targetConfig.getBatchSize).getOrElse(JdbcDefaultOptions.BATCH_SIZE.toString)
    ).++(targetConfig.getOptions)

    val jdbcConfig = buildJdbcConfig(targetDBName, jdbcType, Map("dbtable" -> " ") ++ options)
    val getConnection = createConnectionFactory(new JDBCOptions(jdbcConfig))
    val transactionEnabled = targetConfig.transaction.equalsIgnoreCase(true.toString)
    step.getWriteMode match {
      case APPEND =>
        execute(
          getConnection,
          jdbcConfig,
          dataFrame,
          makeInsertSql(targetTableName, primaryCols, notPrimaryCols),
          primaryCols ++ notPrimaryCols,
          transactionEnabled
        )
      case UPSERT =>
        execute(
          getConnection,
          jdbcConfig,
          dataFrame,
          makeUpsertSql(targetTableName, primaryCols, notPrimaryCols),
          makeUpsertCols(primaryCols, notPrimaryCols),
          transactionEnabled
        )
      case DELETE =>
        execute(
          getConnection,
          jdbcConfig,
          dataFrame,
          makeDeleteSql(targetTableName, primaryCols),
          primaryCols,
          transactionEnabled
        )
      case EXECUTE =>
        execute(
          getConnection,
          jdbcConfig,
          dataFrame,
          step.getSql,
          Nil,
          transactionEnabled
        )
      //  TODO 待修改，修改为基于逻辑主键覆盖，逻辑逐渐靠参数传递
      case OVER_WRITE =>
        getConnection().prepareStatement(s"truncate table $targetTableName;").execute()
        execute(
          getConnection,
          jdbcConfig,
          dataFrame,
          makeInsertSql(targetTableName, primaryCols, notPrimaryCols),
          primaryCols ++ notPrimaryCols,
          transactionEnabled
        )
      case _ =>
        throw new RuntimeException(s"Not Support write mode: ${step.getWriteMode}.")
    }
  }

  private def execute(getConnection: () => Connection,
                      jdbcConfig: Map[String, String],
                      df: DataFrame,
                      sql: String,
                      cols: Seq[StructField],
                      transactionEnabled: Boolean): Unit = {
    execute(
      getConnection,
      jdbcConfig,
      df.rdd,
      sql,
      cols,
      transactionEnabled
    )
  }

  private def execute(getConnection: () => Connection,
                      jdbcConfig: Map[String, String],
                      rdd: RDD[Row],
                      sql: String,
                      cols: Seq[StructField],
                      transactionEnabled: Boolean): Unit = {
    val partitionNum = jdbcConfig.get("numPartitions").map(_.toInt).getOrElse(JdbcDefaultOptions.PARTITION_NUM)
    val saveCount = rdd.sparkContext.longAccumulator
    val startTs = System.currentTimeMillis()
    val batchSize = jdbcConfig.get("batchsize").map(_.toInt).getOrElse(JdbcDefaultOptions.BATCH_SIZE)
    try {
      rdd
        .repartition(partitionNum)
        .foreachPartition(
          savePartition(
            _,
            getConnection,
            batchSize,
            cols,
            sql,
            saveCount,
            transactionEnabled
          )
        )
    } finally {
      val endTs = System.currentTimeMillis()
      ETLLogger.info(
        s"""
           |=======================================
           |sql: $sql
           |count: ${saveCount.value}
           |total time: ${endTs - startTs} ms
           |=======================================
           |""".stripMargin)
    }
  }

  def savePartition(rows: Iterator[Row],
                    getConnection: () => Connection,
                    batchSize: Int,
                    cols: Seq[StructField],
                    sql: String,
                    saveCount: LongAccumulator,
                    transactionEnabled: Boolean
                   ): Unit = {
    try {
      val conn: Connection = getConnection()
      var committed = false
      var rowCount = 0
      val useTransaction = transactionEnabled && isSupportsTransactions(conn)

      try {
        // 根据连接支持的事务级别设置是否自动提交
        if (useTransaction) {
          ETLLogger.info("JDBC transaction enabled for current execution.")
          conn.setAutoCommit(false)
        }
        val stmt: PreparedStatement = conn.prepareStatement(sql)

        try {
          while (rows.hasNext) {
            val row: Row = rows.next()
            fillValueIntoStatement(stmt, row, cols)
            rowCount += 1
            if (rowCount % batchSize == 0) stmt.executeBatch()
          }
          if (rowCount % batchSize > 0) stmt.executeBatch()
        } finally {
          stmt.close()
        }
        if (useTransaction) {
          ETLLogger.info("Committing JDBC transaction...")
          conn.commit()
        }
        committed = true
        if (saveCount != null) saveCount.add(rowCount)
      } finally {
        try {
          if (!committed) {
            if (useTransaction) {
              ETLLogger.info("JDBC transaction not committed, now starting rollback...")
              conn.rollback()
            }
          }
        } finally {
          conn.close()
        }
      }
    }
  }

  def isSupportsTransactions(conn: Connection): Boolean = {
    try {
      conn.getMetaData.supportsDataManipulationTransactionsOnly() ||
        conn.getMetaData.supportsDataDefinitionAndDataManipulationTransactions()
    } catch {
      // 非致命性异常
      case NonFatal(e) =>
        throw e
    }
  }

  def fillValueIntoStatement(stmt: PreparedStatement, row: Row, cols: Seq[StructField]): Unit = {
    cols.zipWithIndex.foreach {
      case (field, index) =>
        stmt.setObject(
          index + 1,
          row.getAs(field.name)
        )
    }
    stmt.addBatch()
  }

  def makeInsertCols(primaryCols: Seq[StructField],
                     notPrimaryCols: Seq[StructField]): Seq[StructField] = {
    primaryCols ++ notPrimaryCols
  }

  def makeInsertSql(tableName: String,
                    primaryCols: Seq[StructField],
                    notPrimaryCols: Seq[StructField]): String = {
    s"""
       |INSERT INTO ${tableName} (${primaryCols.union(notPrimaryCols).map(_.name).mkString(", ")})
       |VALUES (${primaryCols.union(notPrimaryCols).map(_ => "?").mkString(", ")})
       |""".stripMargin
  }

  def getCols(targetDBName: String,
              targetTableName: String): (Seq[StructField], Seq[StructField]) = ???

  def makeUpsertCols(primaryCols: Seq[StructField],
                     notPrimaryCols: Seq[StructField]): Seq[StructField] = ???

  def makeUpsertSql(tableName: String,
                    primaryCols: Seq[StructField],
                    notPrimaryCols: Seq[StructField]): String = ???

  def makeDeleteSql(tableName: String,
                    primaryCols: Seq[StructField]): String = {
    s"""
       |delete from $tableName
       |where ${primaryCols.map(col => col.name.concat(" = ?")).mkString(" and ")}
       |""".stripMargin
  }

}
