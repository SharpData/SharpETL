package com.github.sharpdata.sharpetl.spark.end2end.postgres

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

trait PostgresEtlSuit extends AnyFunSpec
  with should.Matchers
  with SparkSessionTestWrapper
  with DataFrameComparer
  with BeforeAndAfterEach {

  def readTable(dbName:String, port: Int, tableName: String): DataFrame = {
    spark.read
      .format("jdbc")
      .option("url", s"jdbc:postgresql://localhost:$port/$dbName")
      .option("dbtable", tableName)
      .option("user", "postgres")
      .option("password", "postgres")
      .load()
  }

  def execute(sql: String, dbName: String,  port: Int): Boolean = {
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

  def executeMigration(sql: String): Boolean = {
    val url = s"jdbc:mysql://localhost:2333/sharp_etl"
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

  def getTimeStampFromStr(str: String): java.sql.Timestamp = {
    import java.sql.Timestamp
    val parsedDate = YYYY_MM_DD_HH_MM_SS.parse(str)
    new Timestamp(parsedDate.getTime)
  }
}

object PostgresEtlSuit {
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