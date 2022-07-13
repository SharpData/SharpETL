package com.github.sharpdata.sharpetl.core.repository.mapper.mysql

import com.github.sharpdata.sharpetl.core.repository.model.StepLog
import com.github.sharpdata.sharpetl.core.repository.model.StepLog
import org.apache.ibatis.annotations._

trait StepLogMapper extends Serializable {

  @Insert(Array("insert into step_log(" +
    "job_id, step_id, status, start_time, end_time, duration, output, error, " +
    "source_count, target_count, success_count, failure_count, source_type, target_type" +
    ") values (" +
    "#{jobId}, #{stepId}, #{status}, #{startTime}, #{endTime}, #{duration}, #{output}, #{error}, " +
    "#{sourceCount}, #{targetCount}, #{successCount}, #{failureCount}, #{sourceType}, #{targetType}" +
    ")"
  ))
  def createStepLog(stepLog: StepLog): Unit

  @Update(Array(
    "update step_log set " +
      "job_id = #{jobId}, " +
      "step_id = #{stepId}, " +
      "status = #{status}, " +
      "start_time = #{startTime}, " +
      "end_time = #{endTime}, " +
      "duration = #{duration}, " +
      "output = #{output}, " +
      "error = #{error}, " +
      "source_count = #{sourceCount}, " +
      "target_count = #{targetCount}, " +
      "success_count = #{successCount}, " +
      "failure_count = #{failureCount}, " +
      "source_type = #{sourceType}, " +
      "target_type = #{targetType} " +
      "where job_id = #{jobId} and step_id = #{stepId}"
  ))
  def updateStepLog(stepLog: StepLog): Unit

  @Select(Array(
    "select " +
      "job_id, step_id, status, start_time, end_time, duration, output, error, " +
      "source_count, target_count, success_count, failure_count, source_type, target_type" +
      " from sharp_etl.step_log where job_id = #{jobId}"
  ))
  def stepLogs(jobId: Long): Array[StepLog]

  @Select(Array(
    "select " +
      "job_id, step_id, status, start_time, end_time, duration, output, error, " +
      "source_count, target_count, success_count, failure_count, source_type, target_type" +
      " from sharp_etl.step_log where job_id in" +
      " (select job_id from sharp_etl.job_log " +
      " where job_start_time >= #{startTime}" +
      " and job_start_time < #{endTime})"
  ))
  def stepLogsBetween(@Param("startTime") startTime: String, @Param("endTime") endTime: String): Array[StepLog]
}
