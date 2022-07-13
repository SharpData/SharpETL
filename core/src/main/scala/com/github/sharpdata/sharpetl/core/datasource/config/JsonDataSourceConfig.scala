package com.github.sharpdata.sharpetl.core.datasource.config

import com.github.sharpdata.sharpetl.core.annotation.configFor
import com.github.sharpdata.sharpetl.core.util.Constants.BooleanString

import scala.beans.BeanProperty

@configFor(types = Array("json"))
class JsonDataSourceConfig extends FileDataSourceConfig with Serializable {

  @BeanProperty
  var multiline: String = BooleanString.FALSE

  // 默认非严格模式
  @BeanProperty
  var mode: String = "PERMISSIVE"

}
