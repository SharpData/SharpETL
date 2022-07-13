package com.github.sharpdata.sharpetl.spark.transformation

import com.github.sharpdata.sharpetl.spark.end2end.ETLSuit
import com.github.sharpdata.sharpetl.core.util.DateUtil
import ETLSuit.runJob
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.scalatest.DoNotDiscover

import java.time.LocalDateTime
import scala.collection.mutable

@DoNotDiscover
class JdbcTransformerSpec extends ETLSuit {

  val schema = List(
    StructField("number", LongType)
  )

  val expDf = spark.createDataFrame(
    spark.sparkContext.parallelize(Seq(Row(12.toLong)))
    , StructType(schema))

  override val createTableSql: String =
    """
      |create procedure my_test() begin
      |select 12 as 'number';
      |end
      |""".stripMargin

  it("should call sp and return result as dataframe") {
    execute(createTableSql, sourceDbName, "data")
    val df = JdbcResultSetTransformer.transform(
      Map(
        "dbName" -> "int_test",
        "dbType" -> "mysql",
        "sql" -> "call my_test()"
      ))
    assertSmallDataFrameEquality(df, expDf, orderedComparison = false)
  }

  it("should call sp with no return success") {
    execute("create procedure empty_procedure() begin end", sourceDbName, "data")
    val df = JdbcResultSetTransformer.transform(
      Map(
        "dbName" -> "int_test",
        "dbType" -> "mysql",
        "sql" -> "call empty_procedure()"
      ))
  }

  it("should read from sp and write to target") {
    execute("create table sp_test(number bigint)", sourceDbName, "data")
    val startTime = LocalDateTime.now().minusDays(1L).format(DateUtil.L_YYYY_MM_DD_HH_MM_SS)

    val jobParameters: Array[String] = Array("single-job",
      "--name=sp_test", "--period=1440",
      "--local", s"--default-start-time=${startTime}", "--env=test")
    runJob(jobParameters)

    val targetDf = readFromTarget("sp_test")
    assertSmallDataFrameEquality(targetDf, expDf, orderedComparison = false)
  }
}
