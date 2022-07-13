package com.github.sharpdata.sharpetl.spark.transformation

import com.github.sharpdata.sharpetl.spark.end2end.ETLSuit
import ETLSuit.runJob
import org.scalatest.DoNotDiscover

@DoNotDiscover
class DynamicLoadingTransformerSpec extends ETLSuit {
  it("it should execute spark sql in dynamic loading transformer") {
    val jobParameters: Array[String] = Array("single-job",
      "--name=dynamic_transformer", "--period=1440",
      "--local", s"--default-start-time=2021-11-28 15:30:30", "--env=test", "--once")

    runJob(jobParameters)
    val result = spark.sql("select * from `dynamic_tmp_transformer_result_table`")
    result.show()
    result.count() should be(2)
  }

  override val createTableSql: String = ""
}
