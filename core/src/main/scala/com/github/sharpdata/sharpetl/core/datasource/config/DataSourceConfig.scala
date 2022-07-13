package com.github.sharpdata.sharpetl.core.datasource.config

import com.github.sharpdata.sharpetl.core.annotation.Annotations.Stable
import com.github.sharpdata.sharpetl.core.syntax.Formatable
import com.github.sharpdata.sharpetl.core.util.Constants.Separator.ENTER

import scala.beans.BeanProperty

// just for annotation scan
trait DataSourceConf

@Stable(since = "1.0.0")
class DataSourceConfig extends Formatable with DataSourceConf {

  // ConfigFor，For optional values, see [[com.github.sharpdata.sharpetl.core.util.Constants.DataSourceType]]
  @BeanProperty
  var dataSourceType: String = _

  @BeanProperty
  var options: Map[String, String] = Map[String, String]()

  // value of this field should be like this: day:${DAY};month:${MONTH}
  @BeanProperty
  var derivedColumns: String = _

  def optionsToString: String = {
    if (options != null && options.nonEmpty) {
      val builder = new StringBuilder()
      builder.append(s"--  options$ENTER")
      options.foreach { case (key, value) => builder.append(s"--   $key=$value$ENTER") }
      builder.toString()
    } else {
      ""
    }
  }
}
