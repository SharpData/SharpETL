package com.github.sharpdata.sharpetl.core.repository.mapper.mssql

import com.github.sharpdata.sharpetl.core.repository.model.JobLog
import org.apache.ibatis.annotations.{Insert, Options, Param, Select, Update}

/**
 * Logs access for [[LogDrivenJob]]
 */
trait JobLogMapper extends Serializable {
  /**
   * 最新一次执行的任务
   *
   * @return
   */
  @Select(Array(
    "select top 1 " +
      "job_id, job_name, job_period, job_schedule_id, data_range_start, " +
      "data_range_end, job_start_time, job_end_time, status, create_time, last_update_time, current_file, incremental_type, application_id, project_name" +
      " from sharp_etl.job_log where job_name = #{jobName} and status != 'RUNNING' order by data_range_start desc, job_id desc"
  ))
  def lastExecuted(jobName: String): JobLog

  /**
   * 最新一次成功执行的任务
   *
   * @return
   */
  @Select(Array(
    "select top 1 " +
      "job_id, job_name, job_period, job_schedule_id, data_range_start, " +
      "data_range_end, job_start_time, job_end_time, status, create_time, last_update_time, current_file, incremental_type, application_id, project_name" +
      " from sharp_etl.job_log where job_name = #{jobName} and status = 'SUCCESS' order by data_range_start desc"
  ))
  def lastSuccessExecuted(jobName: String): JobLog

  /**
   * job executions in last year
   *
   * @param jobName
   * @return
   */
  @Select(Array(
    "select " +
      "job_id, job_name, job_period, job_schedule_id, data_range_start, " +
      "data_range_end, job_start_time, job_end_time, status, create_time, last_update_time,  current_file, incremental_type, application_id, project_name" +
      " from sharp_etl.job_log where job_name = #{jobName} and status = 'SUCCESS' and job_start_time > #{lastYear}"
  ))
  def executionsLastYear(@Param("jobName") jobName: String, @Param("lastYear") lastYear: String): Array[JobLog]

  /**
   * job executions between
   *
   * @param startTime
   * @param endTime
   * @return
   */
  @Select(Array(
    "select " +
      "job_id, job_name, job_period, job_schedule_id, data_range_start, " +
      "data_range_end, job_start_time, job_end_time, status, create_time, last_update_time,  current_file, incremental_type, application_id, project_name" +
      " from sharp_etl.job_log where job_start_time >= #{startTime} and job_start_time < #{endTime}"
  ))
  def executionsBetween(@Param("startTime") startTime: String, @Param("endTime") endTime: String): Array[JobLog]

  /**
   * 判断是否有另一个相同[[ExecPeriod]]的任务在运行
   *
   * @param jobScheduleId 区分[[ExecPeriod]]
   * @return
   */
  @Select(Array(
    "select top 1 " +
      "job_id, job_name, job_period, job_schedule_id, data_range_start, " +
      "data_range_end, job_start_time, job_end_time, status, create_time, last_update_time,  current_file, incremental_type, application_id, project_name" +
      " from sharp_etl.job_log where job_schedule_id = #{jobScheduleId} and status = 'RUNNING'"
  ))
  def isAnotherJobRunning(jobScheduleId: String): JobLog

  @Insert(Array("insert into sharp_etl.job_log(job_name, job_period, job_schedule_id," +
    "data_range_start, data_range_end," +
    "job_start_time, job_end_time, " +
    "status, create_time," +
    "last_update_time, current_file, application_id, project_name, incremental_type) values (#{jobName}, #{jobPeriod}, #{jobScheduleId}, " +
    "#{dataRangeStart}, #{dataRangeEnd}, #{jobStartTime}, #{jobEndTime}, " +
    "#{status}, #{createTime}, #{lastUpdateTime}, #{currentFile}, #{applicationId}, #{projectName}, #{incrementalType})"
  ))
  @Options(useGeneratedKeys = true, keyProperty = "jobId")
  def createJobLog(jobLog: JobLog): Unit


  @Update(Array(
    "update sharp_etl.job_log set " +
      "job_name = #{jobName}, " +
      "job_period = #{jobPeriod}, " +
      "job_schedule_id = #{jobScheduleId}, " +
      "data_range_start = #{dataRangeStart}, " +
      "data_range_end = #{dataRangeEnd}, " +
      "job_start_time = #{jobStartTime}, " +
      "job_end_time = #{jobEndTime}, " +
      "status = #{status}, " +
      "create_time = #{createTime}, " +
      "last_update_time = #{lastUpdateTime}, " +
      "current_file = #{currentFile}, " +
      "application_id = #{applicationId}, " +
      "incremental_type = #{incrementalType}, " +
      "project_name = #{projectName} " +
      "where job_id = #{jobId}"
  ))
  def updateJobLog(jobLog: JobLog): Unit

  @Select(Array(
    "select top 1 " +
      "job_id, job_name, job_period, job_schedule_id, data_range_start, " +
      "data_range_end, job_start_time, job_end_time, status, create_time, last_update_time, current_file, incremental_type, application_id, project_name" +
      " from sharp_etl.job_log where job_name = #{jobName} and job_start_time < #{jobStartTime} order by job_start_time desc"
  ))
  def lastJobLog(@Param("jobName") jobName: String, @Param("jobStartTime") jobStartTime: String): JobLog

  @Select(Array(
    "select " +
      "job_id, job_name, job_period, job_schedule_id, data_range_start, " +
      "data_range_end, job_start_time, job_end_time, status, create_time, last_update_time, current_file, incremental_type, application_id, project_name" +
      " from job_log where status='SUCCESS' and job_name = #{upstreamJobName} and job_id > #{upstreamLogId} order by job_id"
  ))
  def unprocessedUpstreamJobLog(@Param("upstreamJobName") upstreamJobName: String, @Param("upstreamLogId") upstreamLogId: BigInt): Array[JobLog]
}
