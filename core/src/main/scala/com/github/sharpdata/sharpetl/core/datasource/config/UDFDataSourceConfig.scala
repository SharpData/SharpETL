package com.github.sharpdata.sharpetl.core.datasource.config

import com.github.sharpdata.sharpetl.core.annotation.configFor

import scala.beans.BeanProperty

@configFor(types = Array("udf"))
class UDFDataSourceConfig extends DataSourceConfig with Serializable {

  // 需注册的方法名
  @BeanProperty
  var methodName: String = _

  // 需注册的 udf 名称，注册后可在 sql 中调用此名称执行注册的方法
  @BeanProperty
  var udfName: String = _

}
