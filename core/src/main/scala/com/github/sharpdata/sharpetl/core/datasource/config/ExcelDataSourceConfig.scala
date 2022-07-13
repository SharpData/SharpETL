package com.github.sharpdata.sharpetl.core.datasource.config

import com.github.sharpdata.sharpetl.core.annotation.configFor
import com.github.sharpdata.sharpetl.core.util.Constants.BooleanString

import scala.beans.BeanProperty

@configFor(types = Array("excel"))
class ExcelDataSourceConfig extends FileDataSourceConfig with Serializable {

  // 是否包含表头，默认 true
  @BeanProperty
  var header: String = BooleanString.TRUE

  // 是否转换空值为 null，默认 false
  @BeanProperty
  var treatEmptyValuesAsNulls: String = BooleanString.TRUE

  // 启用结构推断，默认 false
  @BeanProperty
  var inferSchema: String = BooleanString.FALSE

  // 是否额外添加列颜色字段，默认 false
  @BeanProperty
  var addColorColumns: String = BooleanString.FALSE

  // 数据地址，默认 A1 ，可部分设置，只设置 sheet 页或只设置开始单元格位置都可以
  // 例：'Sheet2'!A1:D3
  // sheet 页名称   开始单元格位置   终止单元格位置
  @BeanProperty
  var dataAddress: String = _

  // 默认 yyyy-mm-dd hh:mm:ss[.fffffffff]
  @BeanProperty
  var timestampFormat: String = "MM-dd-yyyy HH:mm:ss"

  // 读取超大文档可设置此值，会使用 streaming reader
  @BeanProperty
  var maxRowsInMemory: String = _

  // 结构推断时使用的数据行数，默认 10
  @BeanProperty
  // scalastyle:off
  var excerptSize: Int = 10
  // scalastyle:on

  // 文档密码，默认 null
  @BeanProperty
  var workbookPassword: String = _

}
