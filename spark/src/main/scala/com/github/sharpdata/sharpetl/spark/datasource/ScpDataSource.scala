package com.github.sharpdata.sharpetl.spark.datasource

import com.github.sharpdata.sharpetl.spark.utils.JSchUtil.{disconnect, exec, getSession}
import com.jcraft.jsch.Session
import com.github.sharpdata.sharpetl.core.datasource.config.TextFileDataSourceConfig
import com.github.sharpdata.sharpetl.core.datasource.connection.ScpConnection
import com.github.sharpdata.sharpetl.core.repository.model.JobLog
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.core.util.{ETLConfig, ETLLogger, HDFSUtil, IOUtil, StringUtil}
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.ivy.plugins.repository.ssh.Scp
import org.apache.spark.sql.{DataFrame, SparkSession}

object ScpDataSource {

  def load(
            spark: SparkSession,
            step: WorkflowStep,
            job: JobLog): DataFrame = {
    val sourceConfig = step.getSourceConfig[TextFileDataSourceConfig]
    val prefix = sourceConfig.getConfigPrefix
    val jschConfig = new ScpConnection(prefix)

    val remoteFilePath = sourceConfig.getFilePath
    val fileName = StringUtil.getFileNameFromPath(remoteFilePath)
    val localTempFilePath = StringUtil.concatFilePath(jschConfig.getLocalTempDir, fileName)
    val hdfsTempFilePath = CompressionCodecFactory.removeSuffix(
      StringUtil.concatFilePath(jschConfig.getHdfsTempDir, fileName),
      sourceConfig.codecExtension
    )

    IOUtil.mkdirs(jschConfig.getLocalTempDir)
    scpFrom(jschConfig, remoteFilePath, localTempFilePath)

    HDFSUtil.mkdirs(jschConfig.getHdfsTempDir)
    HDFSUtil.put(
      localTempFilePath,
      hdfsTempFilePath,
      sourceConfig.getCodecExtension,
      sourceConfig.getDecompress.toBoolean
    )
    IOUtil.delete(localTempFilePath)

    sourceConfig.setFilePath(hdfsTempFilePath)
    //TODO: [[com.github.sharpdata.sharpetl.common.model.LoadFileLog]]
    /*val loadFileLog = new LoadFileLog
    loadFileLog.setFilePath(remoteFilePath)
    loadFileLog.setTaskId(job.jobId)
    if (!source.getOnlyOneName.toBoolean) {
      TaskDataAccessor.newLoadFileLog(loadFileLog)
    }*/
    spark.emptyDataFrame
  }

  def listFilePath(step: WorkflowStep): List[String] = {
    val session = try {
      getSession(step.getSourceConfig[TextFileDataSourceConfig].getConfigPrefix)
    } catch {
      case e: Exception =>
        ETLLogger.error("Get Session failed.", e)
        throw e
    }
    try {
      listFilePath(session, step)
    } finally {
      disconnect(session)
    }
  }

  def listFilePath(session: Session, step: WorkflowStep): List[String] = {
    val sourceConfig = step.getSourceConfig[TextFileDataSourceConfig]
    var dir = ETLConfig.getProperty(s"${sourceConfig.getConfigPrefix}.scp.dir")
    if (sourceConfig.getFileDir != null && sourceConfig.getFileDir != "") {
      dir = sourceConfig.getFileDir
    }
    listFilePath(
      session,
      dir,
      sourceConfig.getFileNamePattern
    )
  }

  def listFilePath(
    session: Session,
    dir: String,
    fileNamePattern: String): List[String] = {
    val command =
      s"""
         |cd $dir
         |ls
         |""".stripMargin
    val ret = exec(session, command)
    ret
      .last
      .split("\n")
      .flatMap(_.split("\t").flatMap(_.split(" ").map(_.trim)))
      .filter(fileName => fileNamePattern.r.findFirstMatchIn(fileName).isDefined)
      .map(fileName => StringUtil.concatFilePath(dir, fileName))
      .toList
  }

  def scpFrom(
    prefix: String,
    remoteFile: String,
    localTarget: String): String = {
    val jschConfig = new ScpConnection(prefix)
    scpFrom(jschConfig, remoteFile, localTarget)
  }

  def scpFrom(
               jschConfig: ScpConnection,
               remoteFile: String,
               localTarget: String): String = {
    val session = getSession(jschConfig)
    val scp = new Scp(session)
    try {
      scpFrom(scp, remoteFile, localTarget)
    } catch {
      case e: Exception =>
        ETLLogger.error(s"Scp from remote '$remoteFile' to local '$localTarget' failed.", e)
        throw e
    } finally {
      disconnect(session)
    }
  }

  def scpFrom(
    session: Session,
    remoteFile: String,
    localTarget: String): String = {
    val scp = new Scp(session)
    scpFrom(scp, remoteFile, localTarget)
  }

  def scpFrom(
    scp: Scp,
    remoteFile: String,
    localTarget: String): String = {
    ETLLogger.info(s"Scp from remote '$remoteFile' to local '$localTarget'")
    scp.get(remoteFile, localTarget)
    localTarget
  }

  def delete(
    prefix: String,
    path: String): Unit = {
    val session = getSession(prefix)
    val command = s"rm -f $path"
    exec(session, command)
    disconnect(session)
  }

  def delete(
    session: Session,
    path: String): Unit = {
    val command = s"rm -f $path"
    exec(session, command)
  }

}
