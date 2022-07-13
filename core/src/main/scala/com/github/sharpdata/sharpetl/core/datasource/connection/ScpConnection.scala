package com.github.sharpdata.sharpetl.core.datasource.connection

import com.github.sharpdata.sharpetl.core.util.{ETLConfig, StringUtil}

import scala.beans.BeanProperty

class ScpConnection(prefix: String) {

  private val _prefix = StringUtil.getPrefix(prefix)

  @BeanProperty
  var host: String = ETLConfig.getProperty(s"${_prefix}scp.host")

  @BeanProperty
  val port: Int = ETLConfig.getProperty(s"${_prefix}scp.port").toInt

  @BeanProperty
  var user: String = ETLConfig.getProperty(s"${_prefix}scp.user")

  @BeanProperty
  var password: String = ETLConfig.getProperty(s"${_prefix}scp.password")

  @BeanProperty
  var dir: String = ETLConfig.getProperty(s"${_prefix}scp.dir")

  @BeanProperty
  var localTempDir: String = ETLConfig.getProperty(s"${_prefix}scp.localTempDir", "/tmp/scp")

  @BeanProperty
  var hdfsTempDir: String = ETLConfig.getProperty(s"${_prefix}scp.hdfsTempDir", "/tmp/scp")

}
