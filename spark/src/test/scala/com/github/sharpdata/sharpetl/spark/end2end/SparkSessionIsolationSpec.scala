package com.github.sharpdata.sharpetl.spark.end2end

import com.github.sharpdata.sharpetl.core.util.WorkflowReader
import ETLSuit.runJob
import org.scalatest.DoNotDiscover
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should

@DoNotDiscover
class SparkSessionIsolationSpec extends AnyFunSpec with should.Matchers {
  it("it should read spark conf correctly") {
    val steps = WorkflowReader.readSteps("session_isolation")

    steps.length should be(3)

    steps.head.conf.isEmpty should be(true)
    steps(1).conf should be(Map(
      "spark.sql.shuffle.partitions" -> "1"
    ))
    steps(2).conf should be(Map(
      "spark.sql.shuffle.partitions" -> "5"
    ))
  }

  it("works with sql config file") {
    val command = Array("single-job",
      "--name=session_isolation", "--period=1440",
      "--local", "--env=test", "--once")

    runJob(command)
  }
}
