package com.github.sharpdata.sharpetl.spark.extra.driver

import org.apache.spark.sql.DataFrame

import java.sql.ParameterMetaData

// scalastyle:off

class SparkJdbcParameterMetaData(val data: DataFrame) extends ParameterMetaData{
  override def getParameterCount: Int = data.columns.length

  override def isNullable(param: Int): Int = ParameterMetaData.parameterNullable

  override def isSigned(param: Int): Boolean = false

  override def getPrecision(param: Int): Int = 0

  override def getScale(param: Int): Int = 0

  override def getParameterType(param: Int): Int = 0

  override def getParameterTypeName(param: Int): String = data.dtypes(param - 1)._2

  override def getParameterClassName(param: Int): String = ""

  override def getParameterMode(param: Int): Int = 0

  override def unwrap[T](iface: Class[T]): T = ???

  override def isWrapperFor(iface: Class[_]): Boolean = ???
}

// scalastyle:on