package com.github.sharpdata.sharpetl.core.repository.mapper.flink

import com.github.sharpdata.sharpetl.core.repository.model.QualityCheckLog
import org.apache.ibatis.annotations.Insert

trait QualityCheckLogMapper {
  @Insert(Array(
    "insert into sharp_etl.quality_check_log(id, job_id, job_name, `column`, data_check_type, ids, error_type, warn_count, " +
      "error_count, create_time, last_update_time)",
    "values ",
    "(#{id}, #{jobId}, #{jobName}, #{column}, #{dataCheckType}, #{ids}, #{errorType}, CAST(#{warnCount} as INT), CAST(#{errorCount} as INT), NOW(), NOW())"
  ))
  def create(jobError: QualityCheckLog): Unit
}
