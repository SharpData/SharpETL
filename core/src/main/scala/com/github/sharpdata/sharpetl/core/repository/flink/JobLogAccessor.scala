package com.github.sharpdata.sharpetl.core.repository.flink

import com.github.sharpdata.sharpetl.core.repository
import com.github.sharpdata.sharpetl.core.repository.MyBatisSession.execute
import com.github.sharpdata.sharpetl.core.repository.mapper.flink.JobLogMapper
import com.github.sharpdata.sharpetl.core.repository.model.JobLog
import com.github.sharpdata.sharpetl.core.util.DateUtil.L_YYYY_MM_DD_HH_MM_SS

import java.time.LocalDateTime
import java.time.LocalDateTime.now

class JobLogAccessor() extends repository.JobLogAccessor() {


  def lastSuccessExecuted(workflowName: String): JobLog = {
    execute[JobLog](sessionValue => {
      val mapper = sessionValue.getMapper(classOf[JobLogMapper])
      mapper.lastSuccessExecuted(workflowName)
    })
  }

  override def lastExecuted(workflowName: String): JobLog = {
    execute[JobLog](sessionValue => {
      val mapper = sessionValue.getMapper(classOf[JobLogMapper])
      mapper.lastExecuted(workflowName)
    })
  }

  def isAnotherJobRunning(jobName: String): JobLog = {
    execute[JobLog](sessionValue => {
      val mapper = sessionValue.getMapper(classOf[JobLogMapper])
      mapper.isAnotherJobRunning(jobName)
    })
  }

  override def create(jobLog: JobLog): Unit = {
    super.create(jobLog)
    execute[JobLog](sessionValue => {
      val mapper = sessionValue.getMapper(classOf[JobLogMapper])
      mapper.createJobLog(jobLog)
      jobLog
    })
  }

  override def update(jobLog: JobLog): Unit = {
    super.update(jobLog)
    execute[JobLog](sessionValue => {
      val mapper = sessionValue.getMapper(classOf[JobLogMapper])
      mapper.updateJobLog(jobLog)
      jobLog
    })
  }

  override def updateStatus(jobLog: JobLog): Unit = {
    super.updateStatus(jobLog)
    update(jobLog)
  }

  override def getLatestSuccessJobLogByNames(wfNames: Array[String]): Array[JobLog] = {
    wfNames.map(name => {
      this.lastSuccessExecuted(name)
    }).filterNot(_ == null)
  }

  override def executionsLastYear(workflowName: String): Array[JobLog] = {
    execute[Array[JobLog]](sessionValue => {
      val mapper = sessionValue.getMapper(classOf[JobLogMapper])
      mapper.executionsLastYear(workflowName, now().minusYears(1L).format(L_YYYY_MM_DD_HH_MM_SS))
    })
  }

  override def executionsBetween(startTime: LocalDateTime, endTime: LocalDateTime): Array[JobLog] = {
    execute[Array[JobLog]](sessionValue => {
      val mapper = sessionValue.getMapper(classOf[JobLogMapper])
      mapper.executionsBetween(startTime.format(L_YYYY_MM_DD_HH_MM_SS), endTime.format(L_YYYY_MM_DD_HH_MM_SS))
    })
  }

  override def getPreviousJobLog(jobLog: JobLog): JobLog = {
    execute[JobLog](sessionValue => {
      val mapper = sessionValue.getMapper(classOf[JobLogMapper])
      mapper.lastJobLog(jobLog.workflowName, jobLog.jobStartTime.format(L_YYYY_MM_DD_HH_MM_SS))
    })
  }

  override def getUnprocessedUpstreamJobLog(upstreamWFName: String, upstreamLogId: BigInt): Array[JobLog] = {
    execute[Array[JobLog]](sessionValue => {
      val mapper = sessionValue.getMapper(classOf[JobLogMapper])
      mapper.unprocessedUpstreamJobLog(upstreamWFName, upstreamLogId.toString())
    })
  }
}
