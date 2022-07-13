package com.github.sharpdata.sharpetl.spark.transformation

import com.github.sharpdata.sharpetl.spark.end2end.ETLSuit
import ETLSuit.runJob
import org.scalatest.DoNotDiscover


@DoNotDiscover
class ReplaceTemplateTableNameSpec extends ETLSuit {

  it("should replace variable") {
    val dataFrame = spark.createDataFrame(Seq((1, "name1"), (2, "name2"))).toDF("id", "name")
    dataFrame.createTempView("temp_source")
    val jobParameters: Array[String] = Array("single-job",
      "--name=replace_template_tablename", "--period=1440",
      "--local", s"--default-start-time=2021-11-28 15:30:30", "--env=test", "--once")

    runJob(jobParameters)
    val list = spark.sql("select * from temp_end").collectAsList()
    val expected = "[[1,name1], [2,name2]]"
    assert(expected == list.toString)
  }
  override val createTableSql: String = ""
}
