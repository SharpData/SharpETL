package com.github.sharpdata.sharpetl.core.datasource.config

import com.github.sharpdata.sharpetl.core.annotation.configFor

@configFor(types = Array("variables"))
class VariableDataSourceConfig extends DataSourceConfig() with Serializable {
  // scalastyle:off
  override def toString: String = {
    this.dataSourceType = "variables"
    val builder = new StringBuilder()

    builder.toString
  }
  // scalastyle:on
}
