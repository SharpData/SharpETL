package com.github.sharpdata.sharpetl.core.repository

import com.github.sharpdata.sharpetl.core.repository.model.StepLog
import com.github.sharpdata.sharpetl.core.util.Constants
import com.github.sharpdata.sharpetl.core.util.Constants.ETLDatabaseType
import com.github.sharpdata.sharpetl.core.repository.model.StepLog
import com.github.sharpdata.sharpetl.core.util.{Constants, JDBCUtil}

import java.time.LocalDateTime

abstract class StepLogAccessor() {

  def create(stepLog: StepLog): Unit

  def update(stepLog: StepLog): Unit

  def stepLogs(jobId: Long): Array[StepLog]

  def stepLogsBetween(startTime: LocalDateTime, endTime: LocalDateTime): Array[StepLog]

}

object StepLogAccessor {
  lazy val stepLogAccessor: StepLogAccessor = StepLogAccessor.getInstance(JDBCUtil.dbType)

  private def getInstance(databaseType: String): StepLogAccessor = {
    databaseType match {
      case ETLDatabaseType.MSSQL => new com.github.sharpdata.sharpetl.core.repository.mssql.StepLogAccessor()
      case Constants.ETLDatabaseType.H2 => new com.github.sharpdata.sharpetl.core.repository.mysql.StepLogAccessor()
      case Constants.ETLDatabaseType.MYSQL => new com.github.sharpdata.sharpetl.core.repository.mysql.StepLogAccessor()
      case Constants.ETLDatabaseType.SPARK_SHARP_ETL => new com.github.sharpdata.sharpetl.core.repository.spark.StepLogAccessor()
    }
  }
}


