package com.github.sharpdata.sharpetl.core.repository

import com.github.sharpdata.sharpetl.core.util.Constants
import com.github.sharpdata.sharpetl.core.util.Constants.ETLDatabaseType
import com.github.sharpdata.sharpetl.core.repository.model.QualityCheckLog

abstract class QualityCheckAccessor() {
  def create(log: QualityCheckLog): Unit
}


object QualityCheckAccessor {
  def getInstance(databaseType: String): QualityCheckAccessor = {
    databaseType match {
      case ETLDatabaseType.MSSQL => new com.github.sharpdata.sharpetl.core.repository.mssql.QualityCheckAccessor()
      case Constants.ETLDatabaseType.H2 => new com.github.sharpdata.sharpetl.core.repository.mysql.QualityCheckAccessor()
      case Constants.ETLDatabaseType.MYSQL => new com.github.sharpdata.sharpetl.core.repository.mysql.QualityCheckAccessor()
      case Constants.ETLDatabaseType.SPARK_SHARP_ETL => new com.github.sharpdata.sharpetl.core.repository.spark.QualityCheckAccessor()
      case Constants.ETLDatabaseType.FLINK_SHARP_ETL => new com.github.sharpdata.sharpetl.core.repository.flink.QualityCheckAccessor()
    }
  }
}
