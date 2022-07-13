package com.github.sharpdata.sharpetl.core.util

import com.github.sharpdata.sharpetl.core.util.StringUtil.BigIntConverter

import java.math.BigInteger
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object DateUtil {
  val YYYY_MM = new SimpleDateFormat("yyyyMM")
  val YYYY_MM_DD = new SimpleDateFormat("yyyyMMdd")
  val YYYY_MM_DD_WITH_DASH = new SimpleDateFormat("yyyy-MM-dd")
  val YYYY_MM_DD_HH = new SimpleDateFormat("yyyyMMddHH")
  val YYYY_MM_DD_HH_MM_SS = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val L_YYYY_MM_DD = DateTimeFormatter.ofPattern("yyyyMMdd")
  val L_YYYY_MM_DD_HH_MM_SS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  val INT_YYYY_MM_DD_HH_MM_SS = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
  // datetime format in job_log table(for `data_range_start` & `data_range_end`)
  val YYYYMMDDHHMMSS = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
  val SPARK_JSON_DATETIME = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS")
  val HH = new SimpleDateFormat("HH")

  implicit class LocalDateTimeToBigInt(localDateTime: LocalDateTime) {
    def asBigInt(): BigInteger = localDateTime.format(YYYYMMDDHHMMSS).asBigInt
  }

  implicit class BigIntToLocalDateTime(value: BigInteger) {
    def asLocalDateTime(): LocalDateTime = LocalDateTime.parse(value.toString(), YYYYMMDDHHMMSS)
  }

  def formatDate(source: String, sourceFormat: SimpleDateFormat, targetFormat: SimpleDateFormat): String = {
    targetFormat.format(sourceFormat.parse(source))
  }
}
