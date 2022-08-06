package com.github.sharpdata.sharpetl.spark.end2end

import com.github.sharpdata.sharpetl.core.util.WorkflowReader
import ETLSuit.runJob
import org.mockito.ArgumentMatchers.anyString
import org.mockito.MockitoSugar.{when, withObjectMocked}
import org.scalatest.DoNotDiscover

/**
 * 1. create deps(task-a, task-b, task-c(depends on task-a, task-b))
 * 2. run task-c (do nothing)
 * 3. run task-a (success)
 * 4. run task-c (do nothing)
 * 5. run task-b (success)
 * 6. run task-c (success)
 */
@DoNotDiscover
class TaskDependenciesSpec extends ETLSuit {
  override val createTableSql: String = ""
  override val targetDbName = "int_test"
  override val sourceDbName: String = "int_test"

  def jobParameters(jobName: String): Array[String] = Array("single-job",
    s"--name=$jobName", "--period=1440",
    "--local", "--env=test", "--once")


  it("should respect to job dependencies") {

    // 1. run jobDependencyCheck (do nothing)
    assertThrows[JobFailedException] {
      runJob(jobParameters("jobDependencyCheck"))
    }

    withObjectMocked[WorkflowReader.type] {
      when(WorkflowReader.readWorkflow(anyString())).thenReturn(workflow("task-a"))
      // 2. run task-a (success)
      runJob(jobParameters("task-a"))
    }

    // 3. run jobDependencyCheck (do nothing)
    assertThrows[JobFailedException] {
      runJob(jobParameters("jobDependencyCheck"))
    }

    withObjectMocked[WorkflowReader.type] {
      when(WorkflowReader.readWorkflow(anyString())).thenReturn(workflow("task-b"))
      // 4. run task-b (success)
      runJob(jobParameters("task-b"))
    }

    // 5. run jobDependencyCheck (do nothing)
    runJob(jobParameters("jobDependencyCheck"))
  }
}
