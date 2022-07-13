package com.github.sharpdata.sharpetl.core.datasource.config

import com.github.sharpdata.sharpetl.core.annotation.configFor
import com.github.sharpdata.sharpetl.core.util.Constants.{BooleanString, Encoding}

import scala.beans.BeanProperty

@configFor(types = Array("ftp", "hdfs", "scp"))
class TextFileDataSourceConfig extends FileDataSourceConfig with Serializable {

  // 文件编码，默认 utf-8
  @BeanProperty
  var encoding: String = Encoding.UTF8

  // 压缩格式后缀，默认不压缩
  @BeanProperty
  var codecExtension: String = ""

  // 是否解压后读取
  @BeanProperty
  var decompress: String = BooleanString.FALSE

  // 分隔符，separator 和 fieldLengthConfig 只需配置一项
  @BeanProperty
  var separator: String = _

  // 各字段长度配置，separator 和 fieldLengthConfig 只需配置一项
  @BeanProperty
  var fieldLengthConfig: String = _

  // 文件列数是否需要严格一致，默认不需要
  // 例如：源文件包含 30 个字段，目标表需要 20 个字段，非严格模式直接取文件前 20 列，严格模式会过滤掉列数不一致的数据行
  @BeanProperty
  var strictColumnNum: String = BooleanString.FALSE

}
