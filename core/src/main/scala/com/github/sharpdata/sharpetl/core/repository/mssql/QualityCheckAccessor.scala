package com.github.sharpdata.sharpetl.core.repository.mssql

import com.github.sharpdata.sharpetl.core.repository.mapper.mssql.QualityCheckLogMapper
import com.github.sharpdata.sharpetl.core.repository
import com.github.sharpdata.sharpetl.core.repository.MyBatisSession.execute
import com.github.sharpdata.sharpetl.core.repository.mapper.mssql
import com.github.sharpdata.sharpetl.core.repository.model.QualityCheckLog

class QualityCheckAccessor() extends repository.QualityCheckAccessor() {
  def create(log: QualityCheckLog): Unit = {
    execute[QualityCheckLog](sessionValue => {
      val mapper = sessionValue.getMapper(classOf[QualityCheckLogMapper])
      mapper.create(log)
      log
    })
  }
}
