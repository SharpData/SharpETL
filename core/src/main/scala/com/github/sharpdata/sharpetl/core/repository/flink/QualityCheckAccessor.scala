package com.github.sharpdata.sharpetl.core.repository.flink

import com.github.sharpdata.sharpetl.core.repository
import com.github.sharpdata.sharpetl.core.repository.MyBatisSession.execute
import com.github.sharpdata.sharpetl.core.repository.mapper.flink
import com.github.sharpdata.sharpetl.core.repository.model.QualityCheckLog

class QualityCheckAccessor() extends repository.QualityCheckAccessor() {
  def create(log: QualityCheckLog): Unit = {
    execute[QualityCheckLog](sessionValue => {
      val mapper = sessionValue.getMapper(classOf[flink.QualityCheckLogMapper])
      mapper.create(log)
      log
    })
  }
}
