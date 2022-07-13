package com.github.sharpdata.sharpetl.core.datasource.config

import com.github.sharpdata.sharpetl.core.annotation.configFor

import scala.beans.BeanProperty

@configFor(types = Array("pmml"))
class PmmlDataSourceConfig extends ClassDataSourceConfig with Serializable {

  // pmml 模型文件名称（包含后缀）
  @BeanProperty
  var pmmlFileName: String = _

}
