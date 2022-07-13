package com.github.sharpdata.sharpetl.spark.end2end

import com.github.sharpdata.sharpetl.spark.end2end.ETLSuit.runJob
import org.scalatest.DoNotDiscover

import scala.jdk.CollectionConverters._

class TestUdfObj extends Serializable {
  def testUdf(value: String): String = {
    s"$value-proceed-by-udf"
  }
}

@DoNotDiscover
class UDFSpec extends ETLSuit with Serializable {

  override val createTableSql: String = ""
  override val sourceDbName: String = "int_test"
  val sourceTableName: String = "test_delta_table"
  override val targetDbName: String = "int_test"

  val firstDay = "2021-10-01 00:00:00"
  val secondDay = "2021-10-02 00:00:00"

  val source2odsParameters: Array[String] = Array("single-job",
    "--name=udf_test", "--period=1440",
    "--local", s"--default-start-time=${firstDay}", "--env=test", "--once")

  it("should call udf") {
    runJob(source2odsParameters)

    spark.sql("select * from udf_result").collectAsList().asScala.head.get(0) should be("input-proceed-by-udf")
  }
}
