package com.github.sharpdata.sharpetl.core.repository.mssql

import com.github.sharpdata.sharpetl.core.repository
import com.github.sharpdata.sharpetl.core.repository.MyBatisSession.execute
import com.github.sharpdata.sharpetl.core.repository.mapper.mssql
import com.github.sharpdata.sharpetl.core.repository.model.StepLog
import com.github.sharpdata.sharpetl.core.util.DateUtil.L_YYYY_MM_DD_HH_MM_SS

import java.time.LocalDateTime

class StepLogAccessor() extends repository.StepLogAccessor() {

  def create(stepLog: StepLog): Unit = {
    execute[StepLog](sessionValue => {
      val mapper = sessionValue.getMapper(classOf[mssql.StepLogMapper])
      mapper.createStepLog(stepLog)
      stepLog
    })
  }

  def update(stepLog: StepLog): Unit = {
    execute[StepLog](sessionValue => {
      val mapper = sessionValue.getMapper(classOf[mssql.StepLogMapper])
      mapper.updateStepLog(stepLog)
      stepLog
    })
  }

  def stepLogs(jobId: Long): Array[StepLog] = {
    execute[Array[StepLog]](sessionValue => {
      val mapper = sessionValue.getMapper(classOf[mssql.StepLogMapper])
      mapper.stepLogs(jobId)
    })
  }

  def stepLogsBetween(startTime: LocalDateTime, endTime: LocalDateTime): Array[StepLog] = {
    execute[Array[StepLog]](sessionValue => {
      val mapper = sessionValue.getMapper(classOf[mssql.StepLogMapper])
      mapper.stepLogsBetween(startTime.format(L_YYYY_MM_DD_HH_MM_SS), endTime.format(L_YYYY_MM_DD_HH_MM_SS))
    })
  }

}
