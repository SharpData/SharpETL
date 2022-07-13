package com.github.sharpdata.sharpetl.core.datasource.config

import com.github.sharpdata.sharpetl.core.annotation.configFor

import scala.beans.BeanProperty

@configFor(types = Array("bigquery"))
class BigQueryDataSourceConfig extends DataSourceConfig with Serializable {

  @BeanProperty
  var system: String = ""
}
