package com.github.sharpdata.sharpetl.spark.utils

// scalastyle:off

import com.github.sharpdata.sharpetl.core.datasource.connection.ScpConnection
import com.github.sharpdata.sharpetl.core.util.ETLLogger

import java.io._
// scalastyle:on
import com.jcraft.jsch.{ChannelExec, JSch, Session}
import org.apache.commons.io.IOUtils
import org.apache.commons.lang.StringUtils

import scala.collection.mutable.ArrayBuffer

// $COVERAGE-OFF$
object JSchUtil {

  def getSession(prefix: String): Session = {
    val jschConfig: ScpConnection = new ScpConnection(prefix)
    getSession(jschConfig)
  }

  def getSession(jschConfig: ScpConnection): Session = {
    getSession(
      jschConfig.getHost,
      jschConfig.getPort,
      jschConfig.getUser,
      jschConfig.getPassword
    )
  }

  def getSession(host: String,
                 port: Int,
                 user: String,
                 password: String): Session = {
    val jsch = new JSch
    val session = jsch.getSession(user, host, port)
    session.setConfig("StrictHostKeyChecking", "no")
    if (password != null) session.setPassword(password)
    session.connect()
    session
  }

  def connect(session: Session, timeout: Int = 0): Unit = {
    if (!session.isConnected) {
      session.connect(timeout)
    }
  }

  def disconnect(session: Session): Unit = {
    session.disconnect()
  }

  def openChannelExec(session: Session): ChannelExec = {
    session.openChannel("exec").asInstanceOf[ChannelExec]
  }

  def closeChannelExec(channelExec: ChannelExec): Unit = {
    if (channelExec != null) {
      channelExec.disconnect()
    }
  }

  def exec(prefix: String, command: String): String = {
    exec(prefix, List(command)).head
  }

  def exec(prefix: String, commands: List[String]): List[String] = {
    val session: Session = getSession(prefix)
    val ret = exec(session, commands)
    ret
  }

  def exec(jschConfig: ScpConnection, command: String): String = {
    exec(jschConfig, List(command)).head
  }

  def exec(jschConfig: ScpConnection, commands: List[String]): List[String] = {
    val session: Session = getSession(jschConfig)
    try {
      exec(session, commands)
    } finally {
      disconnect(session)
    }
  }

  def exec(session: Session, command: String): List[String] = {
    exec(session, List(command))
  }

  def exec(session: Session, commands: List[String]): List[String] = {
    val ret: ArrayBuffer[String] = ArrayBuffer[String]()
    try {
      connect(session)
      for (command <- commands) {
        val channelExec: ChannelExec = openChannelExec(session)
        channelExec.setPty(true)
        ETLLogger.info("command : " + command)
        channelExec.setCommand(command)
        val inputStream: InputStream = channelExec.getInputStream
        val err: InputStream = channelExec.getErrStream
        channelExec.connect()
        ETLLogger.info("stdout : ")
        var output: String = ""
        val bufferSize = 1024
        val buf: Array[Byte] = new Array[Byte](bufferSize)
        var length: Int = 0
        do {
          length = inputStream.read(buf)
          if (length != -1) {
            output += new String(buf, 0, length)
            ETLLogger.info(new String(buf, 0, length))
          }
        } while (length != -1)
        ETLLogger.error("stderr : " + IOUtils.toString(err))
        ret += StringUtils.chop(output)
        closeChannelExec(channelExec)
      }
      ret.toList
    } catch {
      case e: Exception =>
        ETLLogger.error("Exec commands failed.", e)
        throw e
    } finally {
      disconnect(session)
    }
  }

}
// $COVERAGE-ON$
