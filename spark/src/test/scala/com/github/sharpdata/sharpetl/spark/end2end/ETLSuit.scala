package com.github.sharpdata.sharpetl.spark.end2end

import com.github.sharpdata.sharpetl.core.syntax.Workflow
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
  var logDbPort: Int = 2333

  val wf = workflow("workflowName")

  def workflow(name: String) = Workflow(name, "1440", "incremental", "timewindow", null, null, null, -1, null, false, null, Map(), Nil) // scalastyle:off

  def readFromLog(targetTable: String): DataFrame = {
    spark.read
      .format("jdbc")
      .option("url", s"jdbc:mysql://localhost:$logDbPort/sharp_etl")
      .option("dbtable", targetTable)
      .option("user", "admin")
      .option("password", "admin")
      .load()
      .drop("job_id", "job_time")
  }

  def executeInLog(sql: String, dbName: String): Boolean = {
    val url = s"jdbc:mysql://localhost:$logDbPort/$dbName"
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

  def executeMigration(sql: String): Boolean = {
    executeInLog(sql, "sharp_etl")
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