package com.github.sharpdata.sharpetl.spark.end2end

import com.github.sharpdata.sharpetl.core.util.WorkflowReader
import ETLSuit.runJob
import org.mockito.ArgumentMatchers.anyString
import org.mockito.MockitoSugar.{when, withObjectMocked}
import org.scalatest.DoNotDiscover


/**
 * 1. create a running log in `job_log` table
 * 2. run task (skipRunning = true), assert exception thrown
 * 3. run task (skipRunning = false), no exception thrown
 */
@DoNotDiscover
class SkipRunningJobSpec extends ETLSuit {
  override val createTableSql: String = ""
  override val targetDbName = "int_test"
  override val sourceDbName: String = "int_test"

  val firstDay = "2021-10-01 00:00:00"

  def jobParameters(jobName: String): Array[String] = Array("single-job",
    s"--name=$jobName", "--period=1440",
    "--local", s"--default-start-time=$firstDay", "--env=test", "--once")

  it("should kill running job when --skip-running=false") {
    withObjectMocked[WorkflowReader.type] {
      when(WorkflowReader.readWorkflow(anyString())).thenReturn(workflow("task-0"))
      // 1. run migration if needed
      runJob(jobParameters("task-0"))
    }

    // 2. create a running log in `job_log` table
    execute(
      """INSERT INTO job_log VALUES(null,'do_nothing',1440,'do_nothing-20211001000000',20211001000000, 20211002000000,
        |'2021-10-30 19:08:47','2021-10-30 19:08:50','RUNNING','2021-10-30 19:08:47','2021-10-30 19:08:50','datetime', '', 'local-fake-app', '')"""
        .stripMargin, "sharp_etl", "migration")
    // run task (skipRunning = true), assert exception thrown
    assertThrows[JobFailedException] {
      runJob(jobParameters("do_nothing"))
    }
    // run task (skipRunning = false), no exception thrown
    runJob(jobParameters("do_nothing") :+ "--skip-running=false")
  }
}