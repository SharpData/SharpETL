package com.github.sharpdata.sharpetl.core.datasource.config

import com.github.sharpdata.sharpetl.core.annotation.configFor
import com.github.sharpdata.sharpetl.core.util.Constants.BooleanString

import scala.beans.BeanProperty

@configFor(types = Array("sftp", "mount"))
class RemoteFileDataSourceConfig extends FileDataSourceConfig {
  @BeanProperty
  var sourceDir: String = _

  @BeanProperty
  var readAll: String = BooleanString.FALSE

  @BeanProperty
  var tempDestinationDir: String = _

  @BeanProperty
  var tempDestinationDirPermission: String = "rw-rw----"

  @BeanProperty
  var hdfsDir: String = _

  @BeanProperty
  var filterByTime: String = BooleanString.TRUE

  @BeanProperty
  var timeZone: String = "GMT+8"

  // 是否中断后面步骤当数据或文件为空
  @BeanProperty
  var breakFollowStepWhenEmpty: String = BooleanString.TRUE

  @BeanProperty
  var dos2unix: String = BooleanString.FALSE
}
