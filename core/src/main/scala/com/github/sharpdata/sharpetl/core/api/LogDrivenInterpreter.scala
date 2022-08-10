package com.github.sharpdata.sharpetl.core.api

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.github.sharpdata.sharpetl.core.annotation.Annotations.Stable
import com.github.sharpdata.sharpetl.core.cli.{CommonCommand, SingleJobCommand}
import com.github.sharpdata.sharpetl.core.exception.Exception._
import com.github.sharpdata.sharpetl.core.repository.JobLogAccessor
import com.github.sharpdata.sharpetl.core.repository.JobLogAccessor.jobLogAccessor
import com.github.sharpdata.sharpetl.core.repository.model.JobStatus.RUNNING
import com.github.sharpdata.sharpetl.core.repository.model.{JobLog, JobStatus}
import com.github.sharpdata.sharpetl.core.syntax.{Workflow, WorkflowStep}
import com.github.sharpdata.sharpetl.core.util.Constants.IncrementalType
import com.github.sharpdata.sharpetl.core.util.Constants.IncrementalType.UPSTREAM
import com.github.sharpdata.sharpetl.core.util.Constants.Job.nullDataTime
import com.github.sharpdata.sharpetl.core.util.DateUtil.{BigIntToLocalDateTime, LocalDateTimeToBigInt}
import com.github.sharpdata.sharpetl.core.util.IncIdUtil.NumberStringPadding
import com.github.sharpdata.sharpetl.core.util.JobLogUtil.JobLogFormatter
import com.github.sharpdata.sharpetl.core.util.StringUtil.{BigIntConverter, isNullOrEmpty}
import com.github.sharpdata.sharpetl.core.util._

import java.math.BigInteger
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

@Stable(since = "1.0.0")
final case class LogDrivenInterpreter(
                                       workflow: Workflow,
                                       workflowInterpreter: WorkflowInterpreter[_],
                                       jobLogAccessor: JobLogAccessor = jobLogAccessor,
                                       command: CommonCommand
                                     ) {

  @inline def workflowName: String = workflow.name

  private lazy val period = {
    if (command.period > 0) {
      command.period
    } else {
      Option(workflow).map(_.period).map(Option(_)).flatten.map(_.toInt).getOrElse(command.period)
    }
  }

  /**
   * 任务执行主入口
   */
  def interpreting(): WFInterpretingResult = {
    val logs = command match {
      case cmd: CommonCommand if cmd.refresh =>
        executeAndGetResult(unexecutedQueue(Some(cmd.refreshRangeStart), Some(cmd.refreshRangeEnd)), checkRunningAndExecute)
      case _ =>
        if (command.once) {
          executeAndGetResult(unexecutedQueue().headOption.toSeq, checkRunningAndExecute)
        } else if (command.latestOnly) {
          executeAndGetResult(unexecutedQueue().reverse.headOption.toSeq, checkRunningAndExecute)
        } else {
          executeAndGetResult(unexecutedQueue(), checkRunningAndExecute)
        }
    }
    WFInterpretingResult(workflow, logs)
  }

  /**
   * 检查上次运行到那里了，来判断这一次从哪里开始执行
   */
  // scalastyle:off
  def unexecutedQueue(startTimeStr: Option[String] = None, endTimeStr: Option[String] = None): Seq[JobLog] = {
    val lastJob = if (startTimeStr.isDefined && endTimeStr.isDefined) {
      None
    } else {
      Option(jobLogAccessor.lastSuccessExecuted(workflowName))
    }
    val logDrivenType = {
      if (isNullOrEmpty(command.logDrivenType)) {
        Option(workflow).map(_.logDrivenType).getOrElse(IncrementalType.TIMEWINDOW)
      } else {
        command.logDrivenType
      }

    }
    logDrivenType match {
      case _: String if workflow != null && workflow.stopScheduleWhenFail && lastJob.isDefined && lastJob.get.status == JobStatus.FAILURE =>
        ETLLogger.warn("Prev job schedule is failed, and stopScheduleWhenFail is true, so we won't schedule the next run.")
        Seq()
      case IncrementalType.AUTO_INC_ID => autoIncIdBasedQueue(lastJob, logDrivenType, startTimeStr, endTimeStr)
      case IncrementalType.KAFKA_OFFSET => kafkaOffsetBasedQueue(startTimeStr, endTimeStr)
      case IncrementalType.UPSTREAM if !isNullOrEmpty(workflow.upstream) => dependOnUpstreamBasedQueue(lastJob, logDrivenType)
      case _ => timeBasedExecuteQueue(lastJob, logDrivenType, startTimeStr, endTimeStr)
    }
  }
  // scalastyle:on

  private def kafkaOffsetBasedQueue(startTimeStr: Option[String] = None, endTimeStr: Option[String] = None) = {
    type OffsetRange = Map[String, Map[String, Int]]
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val jobLogs = jobLogAccessor.executionsLastYear(workflowName)

    val (dataRangeStart, jobScheduleId) =
      if (startTimeStr.isDefined) {
        //refresh or custom offset
        (startTimeStr.get, s"$workflowName-${startTimeStr.get}")
      } else if (jobLogs.isEmpty) {
        // not exist => from offset 0
        ("earliest", s"$workflowName-earliest")
      } else {
        // exist => OffsetRange => ordered => latest => options
        val dataRangeEnds = jobLogs.toList.map(_.dataRangeEnd).filter(it => it != "earliest")
        if (dataRangeEnds.isEmpty) {
          ("earliest", s"$workflowName-earliest")
        } else {
          val offsetStart = dataRangeEnds
            .map(str => {
              mapper.readValue(str, classOf[OffsetRange])
            })
            .map(it => (it.values.map(_.values.sum).sum, it))
            .maxBy(_._1)
            ._2
          val jobScheduleId = s"$workflowName-${
            offsetStart.map {
              case (topic: String, partitionToOffset: Map[String, Int]) =>
                partitionToOffset.map { case (partition, offset) => s"$topic-$partition-$offset" }
            }
              .mkString("-")
          }"
          (mapper.writeValueAsString(offsetStart), jobScheduleId)
        }
      }

    Seq(
      new JobLog(
        jobId = 0, workflowName = workflowName,
        period = period, jobName = jobScheduleId,
        dataRangeStart = dataRangeStart, dataRangeEnd = endTimeStr.getOrElse("latest"), // update `dataRangeEnd` in [[BatchKafkaDataSource.read()]]
        jobStartTime = nullDataTime, jobEndTime = nullDataTime,
        status = RUNNING, createTime = nullDataTime,
        lastUpdateTime = nullDataTime,
        logDrivenType = IncrementalType.KAFKA_OFFSET,
        file = "", applicationId = workflowInterpreter.applicationId(),
        projectName = workflow.getProjectName(),
        loadType = workflow.loadType,
        runtimeArgs = command.commandStr.toString
      )
    )
  }


  private def autoIncIdBasedQueue(lastJob: Option[JobLog], incrementalType: String,
                                  startTimeStr: Option[String] = None, endTimeStr: Option[String] = None) = {
    val startFrom = startTimeStr.getOrElse {
      lastJob.map(_.dataRangeEnd)
        .getOrElse(Option(command.defaultStart)
          .getOrElse(if (isNullOrEmpty(workflow.defaultStart)) "0" else workflow.defaultStart)
          .padding()
        )
    }
    val jobScheduleId = s"$workflowName-$startFrom"
    Seq(
      new JobLog(
        jobId = 0, workflowName = workflowName,
        period = period, jobName = jobScheduleId,
        dataRangeStart = startFrom.padding(), dataRangeEnd = endTimeStr.getOrElse("0").padding(),
        jobStartTime = nullDataTime, jobEndTime = nullDataTime,
        status = RUNNING, createTime = nullDataTime,
        lastUpdateTime = nullDataTime,
        logDrivenType = incrementalType,
        file = "", applicationId = workflowInterpreter.applicationId(),
        projectName = workflow.getProjectName(),
        loadType = workflow.loadType,
        runtimeArgs = command.commandStr.toString
      )
    )
  }

  private def dependOnUpstreamBasedQueue(lastJob: Option[JobLog], incrementalType: String) = {
    val upstreamJobName = workflow.upstream
    //    1. 先获取本层log数据的upstream_log_id,该数据可以默认从data_range_start获取
    val upstreamLogId = if (lastJob.isDefined) lastJob.get.dataRangeStart.asBigInt else "0".asBigInt
    //    2. 根据command中的upstream的job_name，以及上一步的upstream_log_id，来获取待执行的upstream_log_id
    ETLLogger.info(s"Current job depend on upstream job $upstreamJobName")
    val unprocessedJobLogs = jobLogAccessor.getUnprocessedUpstreamJobLog(upstreamJobName, upstreamLogId)
    //    3. 创建 JobLog对象
    unprocessedJobLogs.map {
      jobLog => {
        dependOnUpstreamScheduleJob(jobLog.jobId, incrementalType)
      }
    }.toList
  }


  private def timeBasedExecuteQueue(lastJob: Option[JobLog], logDrivenType: String,
                                    startTimeStr: Option[String] = None, endTimeStr: Option[String] = None): List[JobLog] = {
    val startTime = startTimeStr.map(str => str.asBigInt.asLocalDateTime()).getOrElse(getStartTime(lastJob))
    val shouldScheduleTimes = ceilingScheduleTimes(startTime, period, endTimeStr)
    if (shouldScheduleTimes == 0) {
      ETLLogger.warn(s"Last job($workflowName)'s data range end is $startTime," +
        s" now is ${LocalDateTime.now()}, there is no plan to schedule next run for job($workflowName)")
    } else if (shouldScheduleTimes > 50) {
      ETLLogger.warn(s"Last job($workflowName)'s data range end is $startTime," +
        s" now is ${LocalDateTime.now()}, there is $shouldScheduleTimes job($workflowName) will to be scheduled.")
    }
    val realLogDrivenType = if (isNullOrEmpty(logDrivenType) ||
      (logDrivenType == UPSTREAM && isNullOrEmpty(workflow.upstream))) {
      ETLLogger.warn(s"logDrivenType was $logDrivenType, will be re-set to ${IncrementalType.TIMEWINDOW}," +
        s" might because upstream job not set, upstream job is ${workflow.upstream}")
      IncrementalType.TIMEWINDOW
    } else {
      logDrivenType
    }
    1.to(shouldScheduleTimes).map { idx =>
      scheduleJob(startTime, period, idx, realLogDrivenType)
    }.toList
  }

  def checkRunningAndExecute(jobLog: JobLog): JobLog = {
    val runningJob = jobLogAccessor.isAnotherJobRunning(jobLog.jobName)
    if (runningJob == null) {
      executeWorkflow(jobLog)
    } else {
      if (!command.skipRunning) {
        runningJob.failed()
        jobLogAccessor.update(runningJob)
        executeWorkflow(jobLog)
      } else {
        throw AnotherJobIsRunningException(s"Exception thrown when another job(${runningJob.jobId}) workflowName: ${runningJob.jobName} is running")
      }
    }
  }

  private def executeWorkflow(jobLog: JobLog): JobLog = {
    ETLLogger.info(s"Executing workflow : ${jobLog.workflowName}")
    try {
      jobLogAccessor.create(jobLog)
      val start = jobLog.formatDataRangeStart()
      val end = jobLog.formatDataRangeEnd()
      val variables = Variables(
        collection.mutable.Map(
          ("${DATA_RANGE_END}", end),
          ("${DATA_RANGE_START}", start),
          ("${JOB_ID}", jobLog.jobId.toString),
          ("${JOB_NAME}", jobLog.jobName),
          ("${WORKFLOW_NAME}", jobLog.workflowName)
        ) ++ jobLog.defaultTimePartition()
      )
      Option(workflow).foreach(wf =>
        ETLLogger.info(s"[Workflow]: \n${wf.headerStr}")
      )
      workflowInterpreter
        .executeSteps(
          dropStep(Option(workflow).map(_.steps).getOrElse(Nil)),
          jobLog,
          variables,
          start,
          end
        )
      jobLog.success()
    } catch {
      case _: NoFileSkipException =>
        ETLLogger.warn("Job won't checkRunningAndExecute any files because there are no files to be proceed and `throwExceptionIfEmpty` is false")
        jobLog.success()
      case e: NoFileToContinueException =>
        ETLLogger.warn("Job can not get any files!")
        jobLog.failed()
        throw e
      case e: StepFailedException =>
        ETLLogger.error(s"Job failed at step ${e.step}", e)
        jobLog.failed()
        throw e
      case e: Throwable =>
        ETLLogger.error(s"Unknown error", e)
        jobLog.failed()
        throw e
    } finally {
      jobLog.jobEndTime = LocalDateTime.now()
      jobLogAccessor.update(jobLog)
    }
    jobLog
  }

  private def scheduleJob(startTime: LocalDateTime, execPeriod: Int, idx: Int, incrementalType: String): JobLog = {
    val dataRangeStart = startTime.plus((idx - 1) * execPeriod, ChronoUnit.MINUTES).asBigInt().toString
    val dataRangeEnd = startTime.plus(idx * execPeriod, ChronoUnit.MINUTES).asBigInt().toString
    val jobScheduleId = s"$workflowName-$dataRangeStart"
    new JobLog(
      jobId = 0, workflowName = workflowName,
      period = execPeriod, jobName = jobScheduleId,
      dataRangeStart = dataRangeStart, dataRangeEnd = dataRangeEnd,
      jobStartTime = nullDataTime, jobEndTime = nullDataTime,
      status = RUNNING, createTime = nullDataTime,
      lastUpdateTime = nullDataTime,
      logDrivenType = incrementalType,
      file = "", applicationId = workflowInterpreter.applicationId(),
      projectName = workflow.getProjectName(),
      loadType = workflow.loadType,
      runtimeArgs = command.commandStr.toString
    )
  }

  private def dependOnUpstreamScheduleJob(upstreamLogId: BigInt, incrementalType: String): JobLog = {
    val dataRangeStart = upstreamLogId.toString()
    val jobScheduleId = s"$workflowName-$dataRangeStart"
    new JobLog(
      jobId = 0, workflowName = workflowName,
      period = period, jobName = jobScheduleId,
      dataRangeStart = dataRangeStart, dataRangeEnd = "",
      jobStartTime = nullDataTime, jobEndTime = nullDataTime,
      status = RUNNING, createTime = nullDataTime,
      lastUpdateTime = nullDataTime,
      logDrivenType = incrementalType,
      file = "", applicationId = workflowInterpreter.applicationId(),
      projectName = workflow.getProjectName(),
      loadType = workflow.loadType,
      runtimeArgs = command.commandStr.toString
    )
  }

  private def getStartTime(lastJob: Option[JobLog]): LocalDateTime = {
    if (lastJob.isEmpty) {
      Option(command.defaultStart)
        .map(
          new BigInteger(_).asLocalDateTime()
        )
        .getOrElse(
          if (isNullOrEmpty(workflow.defaultStart)) {
            LocalDateTime.parse("2022-01-01T00:00:00")
          } else {
            new BigInteger(workflow.defaultStart).asLocalDateTime()
          }
        )
    } else {
      lastJob.get.dataRangeEnd.asBigInt.asLocalDateTime()
    }
  }

  private def ceilingScheduleTimes(startTime: LocalDateTime, execPeriod: Int, endTimeStr: Option[String] = None): Int = {
    Option(execPeriod)
      .filterNot(_ == 0)
      .map(period => startTime.until(endTimeStr.map(str => str.asBigInt.asLocalDateTime()).getOrElse(LocalDateTime.now()), ChronoUnit.MINUTES).toInt / period)
      .getOrElse(1)
  }

  def executeAndGetResult[A](seq: Seq[A], f: A => A): Seq[Try[A]] = {
    @tailrec
    def loop(seq: Seq[A], acc: ListBuffer[Try[A]]): Seq[Try[A]] = {
      seq match {
        case head :: tail =>
          Try(f, head) match {
            case r@Success(_) => loop(tail, acc :+ r)
            case f@Failure(_, _) =>
              acc += f
              tail.foreach(it => acc += Skipped(it))
              acc.toSeq
          }
        case Nil => acc.toSeq
      }
    }

    loop(seq, ListBuffer[Try[A]]())
  }

  def dropStep(steps: List[WorkflowStep]): List[WorkflowStep] = {
    val fromSteps = command match {
      case cmd: SingleJobCommand if !isNullOrEmpty(cmd.fromStep) =>
        steps.slice(cmd.fromStep.toInt - 1, steps.size)
      case _ => steps
    }
    command match {
      case cmd: SingleJobCommand if !isNullOrEmpty(cmd.excludeSteps) =>
        val exclude = cmd.excludeSteps.split(",").map(_.trim).toSet
        fromSteps.filterNot(it => exclude.contains(it.step))
      case _ => fromSteps
    }
  }
}
