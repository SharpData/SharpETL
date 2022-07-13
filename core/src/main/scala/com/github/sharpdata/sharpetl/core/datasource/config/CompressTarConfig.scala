package com.github.sharpdata.sharpetl.core.datasource.config

import com.github.sharpdata.sharpetl.core.annotation.configFor
import com.github.sharpdata.sharpetl.core.util.Constants.Encoding

import scala.beans.BeanProperty

@configFor(types = Array("compresstar"))
class CompressTarConfig extends FileDataSourceConfig with Serializable {

  @BeanProperty
  var encoding: String = Encoding.UTF8

  @BeanProperty
  var targetPath: String = ""

  @BeanProperty
  var tarPath: String = ""

  @BeanProperty
  var bakPath: String = ""

  @BeanProperty
  var tmpPath: String = ""

  @BeanProperty
  var isPassEmptyFile: String = false.toString
}
