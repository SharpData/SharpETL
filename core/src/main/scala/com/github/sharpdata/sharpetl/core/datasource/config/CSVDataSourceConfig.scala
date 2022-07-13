package com.github.sharpdata.sharpetl.core.datasource.config

import com.github.sharpdata.sharpetl.core.annotation.configFor
import com.github.sharpdata.sharpetl.core.util.Constants.{BooleanString, Encoding, Separator}

import scala.beans.BeanProperty

@configFor(types = Array("csv"))
class CSVDataSourceConfig extends FileDataSourceConfig with Serializable {

  @BeanProperty
  var inferSchema: String = BooleanString.TRUE

  @BeanProperty
  var encoding: String = Encoding.UTF8

  @BeanProperty
  var sep: String = Separator.COMMA

  @BeanProperty
  var header: String = BooleanString.TRUE

  @BeanProperty
  var quote: String = "\""

  @BeanProperty
  var escape: String = "\""

  @BeanProperty
  var multiLine: String = BooleanString.FALSE

  @BeanProperty
  var ignoreTrailingWhiteSpace: String = BooleanString.FALSE

  @BeanProperty
  var selectExpr: String = "*"

  @BeanProperty
  var parseTimeFromFileNameRegex: String = ""

  @BeanProperty
  var parseTimeFormatPattern: String = ""

  @BeanProperty
  var parseTimeColumnName: String = "parsedTime"
}
