package com.github.sharpdata.sharpetl.spark.end2end

import com.github.sharpdata.sharpetl.spark.cli.Command
import com.github.sharpdata.sharpetl.spark.job.SparkSessionTestWrapper
import com.github.sharpdata.sharpetl.spark.test.DataFrameComparer
import com.github.sharpdata.sharpetl.core.util.DateUtil.YYYY_MM_DD_HH_MM_SS
import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should
import picocli.CommandLine

import java.sql.{DriverManager, SQLException}
import scala.util.control.NoStackTrace

trait ETLSuit extends AnyFunSpec
  with should.Matchers
  with SparkSessionTestWrapper
  with DataFrameComparer
  with BeforeAndAfterEach {

  val createTableSql: String = ""
  val sourceDbName: String = "int_test"
  val targetDbName: String = "int_test"
  var migrationPort: Int = 2333
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

  def readFromTarget(targetTable: String, dbType: String = "data"): DataFrame = {
    spark.read
      .format("jdbc")
      .option("url", s"jdbc:mysql://localhost:${if (dbType == "migration") migrationPort else dataPort}/${if (dbType == "migration") "sharp_etl" else "int_test"}")
      .option("dbtable", targetTable)
      .option("user", "admin")
      .option("password", "admin")
      .load()
      .drop("job_id", "job_time")
  }

  def execute(sql: String, dbName: String, dbType: String): Boolean = {
    val url = s"jdbc:mysql://localhost:${if (dbType == "migration") migrationPort else dataPort}/$dbName"
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
    execute(sql, targetDbName, "")
  }

  def getTimeStampFromStr(str: String): java.sql.Timestamp = {
    import java.sql.Timestamp
    val parsedDate = YYYY_MM_DD_HH_MM_SS.parse(str)
    new Timestamp(parsedDate.getTime)
  }
}

object ETLSuit {
  private val errorHandler = new CommandLine.IExecutionExceptionHandler() {
    def handleExecutionException(ex: Exception, commandLine: CommandLine, parseResult: CommandLine.ParseResult): Int = {
      ex.printStackTrace()
      commandLine.getCommandSpec.exitCodeOnExecutionException
    }
  }

  def runJob(parameters: Array[String]): Unit = {
    val exitCode = new CommandLine(new Command()).setExecutionExceptionHandler(errorHandler).execute(
      parameters :+ "--release-resource=false": _*
    )

    if (exitCode != 0) {
      throw JobFailedException()
    }
  }
}

final case class JobFailedException() extends RuntimeException with NoStackTrace