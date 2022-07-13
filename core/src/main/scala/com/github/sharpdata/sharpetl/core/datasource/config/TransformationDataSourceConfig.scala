package com.github.sharpdata.sharpetl.core.datasource.config

import com.github.sharpdata.sharpetl.core.annotation.configFor
import com.github.sharpdata.sharpetl.core.util.Constants.Separator.ENTER
import com.github.sharpdata.sharpetl.core.util.Constants.TransformerType
import com.github.sharpdata.sharpetl.core.util.StringUtil

import scala.beans.BeanProperty

@configFor(types = Array("transformation"))
class TransformationDataSourceConfig extends DataSourceConfig with Serializable {
  var className: String = _
  var methodName: String = _
  var transformerType: String = TransformerType.OBJECT_TYPE

  /**
   * Args for function call
   */
  @BeanProperty
  var args: Map[String, String] = _

  // scalastyle:off
  override def toString: String = {
    val builder = new StringBuilder()

    if (!StringUtil.isNullOrEmpty(className)) builder.append(s"--  className=$className$ENTER")
    if (!StringUtil.isNullOrEmpty(methodName)) builder.append(s"--  methodName=$methodName$ENTER")
    if (args != null && args.nonEmpty) {
      args.toList.sortBy(_._1).foreach { case (key, value) => builder.append(s"--   $key=$value$ENTER") }
    }
    if (!StringUtil.isNullOrEmpty(transformerType)) builder.append(s"--  transformerType=$transformerType$ENTER")
    builder.append(optionsToString)
    builder.toString
  }
  // scalastyle:on
}
