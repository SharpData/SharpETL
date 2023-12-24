package com.github.sharpdata.sharpetl.core.repository.model

import com.github.sharpdata.sharpetl.core.util.StringUtil.uuid

import java.time.LocalDateTime
import scala.beans.BeanProperty

//noinspection ScalaStyle
case class QualityCheckLog(
                            @BeanProperty
                            var jobId: String,
                            @BeanProperty
                            var jobName: String,
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
                            var lastUpdateTime: LocalDateTime = LocalDateTime.now(),
                            @BeanProperty
                            var id: String = uuid
                          )
