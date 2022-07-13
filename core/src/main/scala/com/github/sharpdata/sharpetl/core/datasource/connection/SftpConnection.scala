package com.github.sharpdata.sharpetl.core.datasource.connection

import com.github.sharpdata.sharpetl.core.util.{ETLConfig, StringUtil}

class SftpConnection(prefix: String) {

  private val _prefix = StringUtil.getPrefix(prefix)

  val username: String = ETLConfig.getProperty(s"${_prefix}sftp.username")
  val password: String = ETLConfig.getProperty(s"${_prefix}sftp.password")
  val host: String = ETLConfig.getProperty(s"${_prefix}sftp.host")
  val port: Int = ETLConfig.getProperty(s"${_prefix}sftp.port").toInt
  val proxyHost: String = ETLConfig.getProperty(s"${_prefix}sftp.proxyHost")
  val proxyPort: String = ETLConfig.getProperty(s"${_prefix}sftp.proxyPort")
}
