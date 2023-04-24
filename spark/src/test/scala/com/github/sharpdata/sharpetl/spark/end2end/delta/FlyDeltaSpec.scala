package com.github.sharpdata.sharpetl.spark.end2end.delta

import com.github.sharpdata.sharpetl.core.util.ETLLogger
import com.github.sharpdata.sharpetl.spark.end2end.ETLSuit.runJob
import com.github.sharpdata.sharpetl.spark.test.DataFrameComparer
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should
import org.scalatest.{BeforeAndAfterEach, DoNotDiscover}

@DoNotDiscover
class FlyDeltaSpec extends AnyFunSpec
  with should.Matchers
  with DeltaSuit
  with DataFrameComparer
  with BeforeAndAfterEach {

  it("should just run with delta") {
    if (spark.version.startsWith("2.3")) {
      ETLLogger.error("Delta Lake does NOT support Spark 2.3.x")
    } else if (spark.version.startsWith("2.4") || spark.version.startsWith("3.0") || spark.version.startsWith("3.1")|| spark.version.startsWith("3.4")) {
      ETLLogger.error("Delta Lake does not works well on Spark 2.4.x, " +
        "CREATE TABLE USING delta is not supported by Spark before 3.0.0 and Delta Lake before 0.7.0.")
    } else {
      val filePath = getClass.getResource("/application-delta.properties").toString

      val jobParameters: Array[String] = Array("single-job",
        "--name=hello_delta",
        "--local", "--env=test", "--once", s"--property=$filePath")

      runJob(jobParameters)
    }
  }
}
