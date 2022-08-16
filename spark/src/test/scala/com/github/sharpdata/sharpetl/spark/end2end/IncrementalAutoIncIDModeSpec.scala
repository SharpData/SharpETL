package com.github.sharpdata.sharpetl.spark.end2end

import ETLSuit.runJob
import com.github.sharpdata.sharpetl.spark.end2end.mysql.MysqlSuit
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.DoNotDiscover

import java.util.UUID

@DoNotDiscover
class IncrementalAutoIncIDModeSpec extends MysqlSuit {

  override val createTableSql: String =
    "CREATE TABLE IF NOT EXISTS ods_inc_id_table" +
      " (id int, value varchar(255), job_id varchar(255), job_time varchar(255));"

  override val sourceDbName: String = "int_test"
  val sourceTableName: String = "test_inc_id_table"
  override val targetDbName: String = "int_test"

  val source2odsParametersFirstTime: Array[String] = Array("single-job",
    "--name=auto_inc_id_mode", "--period=1440",
    "--local", s"--default-start=9999", "--env=test", "--once")

  val source2odsParametersSecondTime: Array[String] = Array("single-job",
    "--name=auto_inc_id_mode", "--period=1440",
    "--local", "--env=test", "--once")


  val schema = List(
    StructField("id", IntegerType, true),
    StructField("value", StringType, true)
  )

  val firstDayData = (0 until 100).map { idx =>
    Row(10000 + idx, UUID.randomUUID.toString)
  }

  val firstDayDf = spark.createDataFrame(
    spark.sparkContext.parallelize(firstDayData),
    StructType(schema)
  )

  val secondDayData = (0 until 100).map { idx =>
    Row(10100 + idx, UUID.randomUUID.toString)
  }

  val secondDayDf = spark.createDataFrame(
    spark.sparkContext.parallelize(secondDayData),
    StructType(schema)
  )

  it("incremental auto inc id") {
    execute(createTableSql)
    writeDataToSource(firstDayDf, sourceTableName)
    runJob(source2odsParametersFirstTime)
    writeDataToSource(secondDayDf, sourceTableName)
    runJob(source2odsParametersSecondTime)
    val dwdDf = readFromSource("ods_inc_id_table")
    dwdDf.count() should be(200)
  }

  it("refresh auto inc id") {
    val refresh: Array[String] = Array("single-job",
      "--name=auto_inc_id_mode", "--refresh",
      "--local", "--refresh-range-start=10000", "--refresh-range-end=10005", "--env=test", "--once")
    runJob(refresh)
    val dwdDf = readFromSource("ods_inc_id_table")
    dwdDf.count() should be(205)
  }
}
