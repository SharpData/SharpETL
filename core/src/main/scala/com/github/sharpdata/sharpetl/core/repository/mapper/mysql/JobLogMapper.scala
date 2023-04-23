package com.github.sharpdata.sharpetl.core.repository.mapper.mysql

import com.github.sharpdata.sharpetl.core.repository.model.JobLog
import org.apache.ibatis.annotations.{Insert, Options, Param, Select, Update}

/**
 * Logs access for [[LogDrivenJob]]
 */
trait JobLogMapper extends Serializable {

  /**
   * job executions in last year
   *
   * @param workflowName
   * @return
   */
  @Select(Array(
    "select *" +
      " from job_log where workflow_name = #{workflowName} and status = 'SUCCESS' and job_start_time > #{lastYear}"
  ))
  def executionsLastYear(@Param("workflowName") workflowName: String, @Param("lastYear") lastYear: String): Array[JobLog]

  /**
   * job executions between
   *
   * @param
   * @return
   */
  @Select(Array(
    "select *" +
      " from job_log where job_start_time >= #{startTime} and job_start_time < #{endTime}"
  ))
  def executionsBetween(@Param("startTime") startTime: String, @Param("endTime") endTime: String): Array[JobLog]

  /**
   * 最新一次执行的任务
   *
   * @return
   */
  @Select(Array(
    "select *" +
      " from job_log where workflow_name = #{workflowName} and status != 'RUNNING' order by data_range_start desc, job_id desc limit 1"
  ))
  def lastExecuted(jobName: String): JobLog


  /**
   * 最新一次执行的任务
   *
   * @return
   */
  @Select(Array(
    "select *" +
      " from job_log where workflow_name = #{workflowName} and status = 'SUCCESS' order by data_range_start desc limit 1"
  ))
  def lastSuccessExecuted(jobName: String): JobLog

  /**
   * 判断是否有另一个相同[[ExecPeriod]]的任务在运行
   *
   * @param jobName 区分[[ExecPeriod]]
   * @return
   */
  @Select(Array(
    "select *" +
      " from job_log where job_name = #{workflowName} and status = 'RUNNING' limit 1"
  ))
  def isAnotherJobRunning(jobName: String): JobLog

  @Insert(Array("insert into job_log(job_id, job_name, `period`, workflow_name," +
    "data_range_start, data_range_end," +
    "job_start_time, job_end_time, " +
    "status, create_time," +
    "last_update_time, file, application_id, project_name, load_type, log_driven_type, runtime_args) values " +
    "(#{jobId}, #{jobName}, #{period}, #{workflowName}, " +
    "#{dataRangeStart}, #{dataRangeEnd}, #{jobStartTime}, #{jobEndTime}, " +
    "#{status}, #{createTime}, #{lastUpdateTime}, #{file}, #{applicationId}, #{projectName}, #{loadType}, #{logDrivenType}, #{runtimeArgs})"
  ))
  //@Options(useGeneratedKeys = true, keyProperty = "jobId")
  def createJobLog(jobLog: JobLog): Unit

  @Update(Array(
    "update job_log set " +
      "workflow_name = #{workflowName}, " +
      "`period` = #{period}, " +
      "job_name = #{jobName}, " +
      "data_range_start = #{dataRangeStart}, " +
      "data_range_end = #{dataRangeEnd}, " +
      "job_start_time = #{jobStartTime}, " +
      "job_end_time = #{jobEndTime}, " +
      "status = #{status}, " +
      "create_time = #{createTime}, " +
      "last_update_time = #{lastUpdateTime}, " +
      "file = #{file}, " +
      "application_id = #{applicationId}, " +
      "load_type = #{loadType}, " +
      "log_driven_type = #{logDrivenType}, " +
      "project_name = #{projectName}, " +
      "runtime_args = #{runtimeArgs} " +
      "where job_id = #{jobId}"
  ))
  def updateJobLog(jobLog: JobLog): Unit

  @Select(Array(
    "select *" +
      " from job_log where workflow_name = #{workflowName} and job_start_time < #{jobStartTime} order by job_start_time desc limit 1"
  ))
  def lastJobLog(@Param("workflowName") workflowName: String, @Param("jobStartTime") jobStartTime: String): JobLog

  @Select(Array(
    "select *" +
      " from job_log where status='SUCCESS' and workflow_name = #{upstreamWFName} and job_id > #{upstreamLogId} order by job_id"
  ))
  def unprocessedUpstreamJobLog(@Param("upstreamWFName") upstreamWFName: String, @Param("upstreamLogId") upstreamLogId: String): Array[JobLog]
}
