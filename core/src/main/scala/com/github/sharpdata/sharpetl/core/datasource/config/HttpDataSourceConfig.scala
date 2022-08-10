package com.github.sharpdata.sharpetl.core.datasource.config

import com.github.sharpdata.sharpetl.core.annotation.configFor

import scala.beans.BeanProperty

@configFor(types = Array("http"))
class HttpDataSourceConfig extends DataSourceConfig with Serializable {

  @BeanProperty
  var connectionName: String = _

  @BeanProperty
  var url: String = _

  @BeanProperty
  var httpMethod: String = "GET"

  @BeanProperty
  var timeout: String = _

  @BeanProperty
  var requestBody: String = _

  @BeanProperty
  var fieldName: String = "value"

  @BeanProperty
  var jsonPath: String = "$"

  @BeanProperty
  var splitBy: String = ""
}

@configFor(types = Array("http_file"))
class HttpFileDataSourceConfig extends HttpDataSourceConfig {

  @BeanProperty
  var tempDestinationDir = "/tmp"

  @BeanProperty
  var hdfsDir = "/tmp"
}
