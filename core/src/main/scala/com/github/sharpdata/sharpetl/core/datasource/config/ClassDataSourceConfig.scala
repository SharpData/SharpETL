package com.github.sharpdata.sharpetl.core.datasource.config

import com.github.sharpdata.sharpetl.core.annotation.configFor

import scala.beans.BeanProperty

@configFor(types = Array("class", "object"))
class ClassDataSourceConfig extends DataSourceConfig {

  /**
   * scala 类的 class path，例如：[[com.github.sharpdata.sharpetl.spark.udf.PmmlUDF]]
   */
  @BeanProperty
  var className: String = _

}
