package com.github.sharpdata.sharpetl.core.datasource.connection

import com.github.sharpdata.sharpetl.core.util.{ETLConfig, StringUtil}

import scala.beans.BeanProperty

class FtpConnection(prefix: String = "") {

  private val _prefix = StringUtil.getPrefix(prefix)

  @BeanProperty
  var host: String = ETLConfig.getProperty(s"${_prefix}ftp.host")

  @BeanProperty
  var port: Int = ETLConfig.getProperty(s"${_prefix}ftp.port").toInt

  @BeanProperty
  var user: String = ETLConfig.getProperty(s"${_prefix}ftp.user")

  @BeanProperty
  var password: String = ETLConfig.getProperty(s"${_prefix}ftp.password")

  @BeanProperty
  var dir: String = ETLConfig.getProperty(s"${_prefix}ftp.dir")

  @BeanProperty
  var localTempDir: String = ETLConfig.getProperty(s"${_prefix}ftp.localTempDir", "/tmp/ftp")

  @BeanProperty
  var hdfsTempDir: String = ETLConfig.getProperty(s"${_prefix}ftp.hdfsTempDir", "/tmp/ftp")

  val url = s"ftp://$user:$password@$host:$port"

}
