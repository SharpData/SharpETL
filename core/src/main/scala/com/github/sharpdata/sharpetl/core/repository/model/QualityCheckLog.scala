package com.github.sharpdata.sharpetl.core.repository.model

import java.time.LocalDateTime
import scala.beans.BeanProperty

case class QualityCheckLog(
                            @BeanProperty
                            var jobId: Long,
                            @BeanProperty
                            var jobScheduleId: String,
                            @BeanProperty
                            var column: String,
                            @BeanProperty
                            var dataCheckType: String,
                            @BeanProperty
                            var ids: String,
                            @BeanProperty
                            var errorType: String,
                            @BeanProperty
                            var warnCount: Long,
                            @BeanProperty
                            var errorCount: Long,
                            @BeanProperty
                            var createTime: LocalDateTime = LocalDateTime.now(),
                            @BeanProperty
                            var lastUpdateTime: LocalDateTime = LocalDateTime.now())
