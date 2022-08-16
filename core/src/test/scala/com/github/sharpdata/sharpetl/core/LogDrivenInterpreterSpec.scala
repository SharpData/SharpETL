package com.github.sharpdata.sharpetl.core

import com.github.sharpdata.sharpetl.core.api.LogDrivenInterpreter
import com.github.sharpdata.sharpetl.core.cli.SingleJobCommand
import com.github.sharpdata.sharpetl.core.repository.JobLogAccessor
import com.github.sharpdata.sharpetl.core.repository.model.JobLog
import com.github.sharpdata.sharpetl.core.repository.model.JobStatus._
import com.github.sharpdata.sharpetl.core.syntax.Workflow
import com.github.sharpdata.sharpetl.core.test.FakeWorkflowInterpreter
import com.github.sharpdata.sharpetl.core.util.Constants.IncrementalType
import com.github.sharpdata.sharpetl.core.util.Constants.Job.nullDataTime
import com.github.sharpdata.sharpetl.core.util.DateUtil.LocalDateTimeToBigInt
import com.github.sharpdata.sharpetl.core.util.StringUtil.BigIntConverter
import org.mockito.ArgumentMatchers.anyString
import org.mockito.MockitoSugar.{mock, when}
import org.scalatest.flatspec._
import org.scalatest.matchers._
import org.scalatest.prop.TableDrivenPropertyChecks._

import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

class TestJobCommand() extends SingleJobCommand() {
  override def run(): Unit = ()
}

class LogDrivenInterpreterSpec extends AnyFlatSpec with should.Matchers {
  private val now: LocalDateTime = LocalDateTime.now()

  private val execPeriod =
    Table(
      "timeUnit",
      60 * 24 * 30,
      60 * 24,
      60,
      1
    )

  forAll(execPeriod) { period =>

    it should s"not schedule when last run 1 sec ago: $period" in {
      val prevDataEndTime = now.minus(1L * period, ChronoUnit.SECONDS)
      val logDrivenJob: LogDrivenInterpreter = setup(prevDataEndTime, period)
      val unexecutedQueue = logDrivenJob.logDrivenPlan()
      unexecutedQueue.isEmpty should be(true)
    }

    it should s"schedule 1 job: $period" in {
      val prevDataEndTime = now.minus(1L * period, ChronoUnit.MINUTES)
      val logDrivenJob: LogDrivenInterpreter = setup(prevDataEndTime, period)
      val unexecutedQueue = logDrivenJob.logDrivenPlan()
      unexecutedQueue.size should be(1)
      unexecutedQueue.head.dataRangeStart.asBigInt should be(prevDataEndTime.asBigInt())
      unexecutedQueue.head.dataRangeEnd.asBigInt should be(prevDataEndTime.plus(1L * period, ChronoUnit.MINUTES).asBigInt())

      beforeOrEqual(unexecutedQueue.head.dataRangeEnd, now.asBigInt()) should be(true)
    }


    it should s"schedule 1 job when last job run 1 time unit and 1 secs ago: $period" in {

      val prevDataEndTime = now.minus(1 * period, ChronoUnit.MINUTES).minus(1, ChronoUnit.SECONDS)
      val logDrivenJob: LogDrivenInterpreter = setup(prevDataEndTime, period)
      val unexecutedQueue = logDrivenJob.logDrivenPlan()
      unexecutedQueue.size should be(1)
      unexecutedQueue.head.dataRangeStart.asBigInt should be(prevDataEndTime.asBigInt())
      unexecutedQueue.head.dataRangeEnd.asBigInt should be(prevDataEndTime.plus(1L * period, ChronoUnit.MINUTES).asBigInt())

      beforeOrEqual(unexecutedQueue.head.dataRangeEnd, now.asBigInt()) should be(true)
    }


    it should s"schedule 2 job when last job run 2 time unit ago: $period" in {
      val prevDataEndTime = now.minus(2 * period, ChronoUnit.MINUTES)
      val logDrivenJob: LogDrivenInterpreter = setup(prevDataEndTime, period)
      val unexecutedQueue = logDrivenJob.logDrivenPlan()
      unexecutedQueue.size should be(2)
      unexecutedQueue.head.dataRangeStart.asBigInt should be(prevDataEndTime.asBigInt())
      unexecutedQueue.head.dataRangeEnd.asBigInt should be(prevDataEndTime.plus(1L * period, ChronoUnit.MINUTES).asBigInt())
      unexecutedQueue.tail.head.dataRangeStart.asBigInt should be(prevDataEndTime.plus(1L * period, ChronoUnit.MINUTES).asBigInt())
      unexecutedQueue.tail.head.dataRangeEnd.asBigInt should be(prevDataEndTime.plus(2L * period, ChronoUnit.MINUTES).asBigInt())

      beforeOrEqual(unexecutedQueue.tail.head.dataRangeEnd, now.asBigInt()) should be(true)
    }

  }

  private def beforeOrEqual(end: LocalDateTime, now: LocalDateTime) = {
    end.compareTo(now) <= 0
  }

  private def beforeOrEqual(end: BigInt, now: BigInt) = {
    end - now <= 0
  }

  private def beforeOrEqual(end: String, now: BigInt) = {
    new BigInt(end.asBigInt) - now <= 0
  }

  private def setup(prevDataEndTime: LocalDateTime, execPeriod: Int) = {
    val jobLogAccessor = mock[JobLogAccessor]
    mockJobLogAccessor(jobLogAccessor, prevDataEndTime, 24 * 60)
    val command = new TestJobCommand()
    command.once = true
    val logDrivenJob = LogDrivenInterpreter(
      Workflow("workflowName", execPeriod.toString, "incremental", "timewindow", null, null, null, -1, null, false, null, Map(), Nil), // scalastyle:off
      new FakeWorkflowInterpreter(),
      jobLogAccessor = jobLogAccessor,
      command = command
    )
    logDrivenJob
  }

  private def mockJobLogAccessor(jobLogAccessor: JobLogAccessor, prevDataEndTime: LocalDateTime, execPeriod: Int): Any = {
    when(jobLogAccessor.lastSuccessExecuted("workflowName")).thenReturn(
      new JobLog(
        jobId = 0, workflowName = "workflowName",
        period = execPeriod, jobName = "workflowName",
        dataRangeStart = "0", dataRangeEnd = prevDataEndTime.asBigInt().toString,
        jobStartTime = nullDataTime, jobEndTime = nullDataTime,
        status = RUNNING, createTime = now,
        lastUpdateTime = now,
        "",
        IncrementalType.TIMEWINDOW, "","fake-app-001", "project", ""
      )
    )
    when(jobLogAccessor.isAnotherJobRunning(anyString())).thenReturn(null)
  }
}
