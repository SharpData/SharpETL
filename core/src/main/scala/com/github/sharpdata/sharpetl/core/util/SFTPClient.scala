package com.github.sharpdata.sharpetl.core.util

import com.github.sharpdata.sharpetl.core.datasource.connection.SftpConnection
import com.jcraft.jsch.{ChannelSftp, JSch, ProxyHTTP, Session}

import java.io.File
import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermission
import java.util
import java.util.Properties

class SFTPClient(configuration: SftpConnection) {

  var channelSftp: ChannelSftp = _
  var session: Session = _
  val SFTP: String = "sftp"

  def listFiles(path: String, filter: ChannelSftp#LsEntry => Boolean): List[String] = {
    initSFPTChannel()

    var fileNames: List[ChannelSftp#LsEntry] = List()
    channelSftp.ls(path, new ChannelSftp.LsEntrySelector() {
      override def select(entry: ChannelSftp#LsEntry): Int = {
        if (filter.apply(entry)) {
          fileNames = fileNames :+ entry
        }
        0
      }
    })
    val fileNameToMtime = fileNames.map(entry => (entry.getFilename, entry.getAttrs.getMTime))
    fileNameToMtime.map(tuple => tuple._1)
  }

  def downloadFileToLocal(fileName: String, sourceDir: String, destination: String, permissions: util.Set[PosixFilePermission]): Unit = {
    if (!new File(destination).exists()) {
      new File(destination).mkdirs()
    }
    initSFPTChannel()
    val sftpPath = StringUtil.concatFilePath(sourceDir, fileName)
    ETLLogger.info(s"Downloading from SFTP path $sftpPath to local path $destination/$fileName...")
    channelSftp.get(sftpPath, destination)
    Files.setPosixFilePermissions(new File(s"$destination/$fileName").toPath, permissions)
  }

  def close(): Unit = {
    if (channelSftp != null && channelSftp.isConnected) {
      channelSftp.disconnect()
    }

    if (session != null && session.isConnected) {
      session.disconnect()
    }
  }

  private def initSFPTChannel(): Unit = {
    if (channelSftp == null || !channelSftp.isConnected) {
      initSession()
      if (!session.isConnected) {
        session.connect()
      }
      val channel = session.openChannel(SFTP)
      channelSftp = channel.asInstanceOf[ChannelSftp]
      channelSftp.connect()
    }
  }

  private def initSession(): Unit = {
    if (session == null) {
      val jSch = new JSch()
      session = jSch.getSession(configuration.username, configuration.host, configuration.port)
      session.setPassword(configuration.password)
      val properties = new Properties()
      properties.setProperty("StrictHostKeyChecking", "no")
      session.setConfig(properties)
      if (configuration.proxyHost != null) {
        session.setProxy(new ProxyHTTP(configuration.proxyHost, configuration.proxyPort.toInt))
      }
    }
  }
}
