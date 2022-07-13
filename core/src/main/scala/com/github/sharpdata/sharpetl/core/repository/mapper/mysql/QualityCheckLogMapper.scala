package com.github.sharpdata.sharpetl.core.repository.mapper.mysql

import com.github.sharpdata.sharpetl.core.repository.model.QualityCheckLog
import org.apache.ibatis.annotations.Insert

trait QualityCheckLogMapper {
  @Insert(Array(
    "insert into quality_check_log(job_id, job_schedule_id, `column`, data_check_type, ids, error_type, warn_count, " +
      "error_count, create_time, last_update_time)",
    "values ",
    "(#{jobId}, #{jobScheduleId}, #{column}, #{dataCheckType}, #{ids}, #{errorType}, #{warnCount}, #{errorCount}, #{createTime}, #{lastUpdateTime})"
  ))
  def create(jobError: QualityCheckLog): Unit
}
