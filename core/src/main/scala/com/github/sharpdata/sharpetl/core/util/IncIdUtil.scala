package com.github.sharpdata.sharpetl.core.util

object IncIdUtil {
  implicit class NumberStringPadding(value: String) {
    // scalastyle:off
    def padding(size: Int = 12): String = {
      value.reverse.padTo(size, '0').reverse
    }

    // taken from https://stackoverflow.com/a/2800839/5597803
    def trimPadding(): String = {
      value.replaceFirst("^0+(?!$)", "")
    }
    // scalastyle:on
  }
}
