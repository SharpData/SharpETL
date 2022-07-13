package com.github.sharpdata.sharpetl.spark.utils

import org.apache.spark.sql.Column

object ConvertUtils {
  def strsToColumns: Seq[String] => Seq[Column] = _.map(new Column(_))

  def strToColumn: String => Column = new Column(_)
}
