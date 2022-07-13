package com.github.sharpdata.sharpetl.core.extension

import javax.annotation.Nullable


/**
 * [[com.github.sharpdata.sharpetl.spark.extension.Initializer]]
 */
object BuiltInFunctions extends Serializable {
  /**
   * All value of UDF might be null, suggest using @[[javax.annotation.Nullable]]
   * <a href="https://dotty.epfl.ch/docs/reference/other-new-features/explicit-nulls.html#java-interoperability"/>
   */
  def powerNullCheck(@Nullable value: String): Boolean = {
    value == null || value.trim.equalsIgnoreCase("null")
  }

  def flatten(nestedArray: Seq[Seq[String]]): Seq[String] = nestedArray.flatten

  def arrayJoin(array: Seq[String], sep: String): String = {
    array.mkString(sep)
  }

  def top(array: Seq[String], n: Int): Seq[String] = {
    array.take(n)
  }

  def ifEmpty(@Nullable value: String, default: String): String = {
    if (value == null || value.isEmpty) {
      default
    } else {
      value
    }
  }
}
