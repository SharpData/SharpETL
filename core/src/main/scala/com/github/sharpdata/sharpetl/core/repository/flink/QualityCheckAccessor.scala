package com.github.sharpdata.sharpetl.core.repository.flink

import com.github.sharpdata.sharpetl.core.repository
import com.github.sharpdata.sharpetl.core.repository.MyBatisSession.execute
import com.github.sharpdata.sharpetl.core.repository.mapper.spark
import com.github.sharpdata.sharpetl.core.repository.model.QualityCheckLog

class QualityCheckAccessor() extends repository.QualityCheckAccessor() {
  def create(log: QualityCheckLog): Unit = {
    execute[QualityCheckLog](sessionValue => {
      val mapper = sessionValue.getMapper(classOf[spark.QualityCheckLogMapper])
      mapper.create(log)
      log
    })
  }
}
