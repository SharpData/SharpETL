package com.github.sharpdata.sharpetl.spark.datasource

import com.github.sharpdata.sharpetl.core.api.Variables
import com.github.sharpdata.sharpetl.core.datasource.Sink
import com.github.sharpdata.sharpetl.core.datasource.config.TextFileDataSourceConfig
import com.github.sharpdata.sharpetl.core.datasource.connection.FtpConnection
import com.github.sharpdata.sharpetl.core.repository.model.JobLog
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.core.util.Constants.Encoding
import com.github.sharpdata.sharpetl.core.util.{ETLLogger, HDFSUtil, IOUtil, StringUtil}
import com.github.sharpdata.sharpetl.core.annotation._
import org.apache.commons.net.ftp._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io._

@sink(types = Array("ftp"))
class FtpDataSource extends Sink[DataFrame] {

  override def sink(df: DataFrame, step: WorkflowStep, variables: Variables): Unit = save(step)

  def closeFTPClient(ftpClient: FTPClient): Unit = {
    if (ftpClient != null) {
      try {
        ftpClient.logout()
      } catch {
        case e: Exception =>
          ETLLogger.error("close FTP failed.", e)
      } finally {
        if (ftpClient.isConnected) {
          try {
            ftpClient.disconnect()
          } catch {
            case e: IOException =>
              ETLLogger.error("close FTP failed.", e)
          }
        }
      }
    }
  }

  def getFTPClient(configPrefix: String): FTPClient = {
    val config = new FtpConnection(configPrefix)
    getFTPClient(config)
  }

  def getFTPClient(ftpConfig: FtpConnection): FTPClient = {
    getFTPClient(
      ftpConfig.host,
      ftpConfig.port,
      ftpConfig.user,
      ftpConfig.password
    )
  }

  def getFTPClient(host: String,
                   port: Int,
                   user: String,
                   password: String): FTPClient = {
    val ftp: FTPClient = new FTPClient
    ftp.connect(host, port)
    ftp.login(user, password)
    ftp.setConnectTimeout("50000".toInt)
    ftp.setControlEncoding(Encoding.UTF8)
    ftp.setAutodetectUTF8(true)
    ftp.enterLocalPassiveMode()
    ftp.setFileType(FTP.BINARY_FILE_TYPE)
    if (!FTPReply.isPositiveCompletion(ftp.getReplyCode)) {
      ftp.disconnect()
      throw new RuntimeException("connect FTP failed.")
    } else {
      ETLLogger.info("FTP connect success.")
    }
    ftp
  }

  def listFileUrl(ftpConfig: FtpConnection, fileNamePattern: String): List[String] = {
    val ftp = getFTPClient(ftpConfig)
    val files: List[String] = listFile(ftp, ftpConfig.dir, fileNamePattern)
      .map(fileName => s"${ftpConfig.dir}/$fileName")
    closeFTPClient(ftp)
    files
  }

  def listFile(configPrefix: String, fileDir: String, fileNamePattern: String): List[String] = {
    val ftp = getFTPClient(configPrefix)
    val files: List[String] = listFile(ftp, fileDir, fileNamePattern)
    closeFTPClient(ftp)
    files
  }

  def listFile(ftp: FTPClient, fileDir: String, fileNamePattern: String): List[String] = {
    ftp
      .listFiles(
        fileDir,
        new FTPFileFilter {
          override def accept(file: FTPFile): Boolean =
            fileNamePattern.r.findFirstMatchIn(file.getName).isDefined
        }
      )
      .map(_.getName)
      .toList
  }

  def delete(configPrefix: String, path: String): Boolean = {
    val ftp = getFTPClient(configPrefix)
    try {
      delete(ftp, path)
    } catch {
      case e: Exception =>
        ETLLogger.error(s"Delete file '$path' from ftp failed.", e)
        throw e
    } finally {
      ETLLogger.info(s"Delete file '$path' from ftp success.")
      closeFTPClient(ftp)
    }
  }

  def delete(ftp: FTPClient, path: String): Boolean = {
    ftp.deleteFile(path)
  }

  def save(step: WorkflowStep): Unit = {
    val sourceConfig: TextFileDataSourceConfig = step.getSourceConfig[TextFileDataSourceConfig]
    val prefix = sourceConfig.getConfigPrefix
    val ftpConfig = new FtpConnection(prefix)
    val localPath = sourceConfig.getFilePath
    val targetConfig = step.getTargetConfig[TextFileDataSourceConfig]
    val remote = targetConfig.getFilePath
    val fs = HDFSUtil.getFileSystem()
    val local = fs.open(new Path(localPath))
    ftpTo(ftpConfig, local, remote)
    local.close()
  }

  def ftpTo(ftpConfig: FtpConnection, local: InputStream, remote: String): Unit = {
    val ftpClient = getFTPClient(ftpConfig)
    ftpTo(ftpClient, local, remote)
    closeFTPClient(ftpClient)
  }

  def mkdir(ftp: FTPClient, path: String): Boolean = {
    val parentPath = StringUtil.getParentPath(path)
    if (ftp.listDirectories(parentPath).isEmpty) {
      mkdir(ftp, parentPath)
    }
    if (ftp.listDirectories(path).isEmpty) {
      ftp.makeDirectory(path)
    } else {
      true
    }
  }

  def ftpTo(ftp: FTPClient, local: InputStream, remote: String): Unit = {
    ftp.deleteFile(remote)
    val fildDir = StringUtil.getParentPath(remote)
    mkdir(ftp, fildDir)
    val tempFilePath = StringUtil.concatFilePath(fildDir, StringUtil.uuid)
    val success = ftp.storeFile(tempFilePath, local)
    ftp.rename(tempFilePath, remote)
    if (success) {
      ETLLogger.info(s"Upload file to '$remote' success.")
    } else {
      val errMsg = s"Upload file to '$remote' failed."
      ETLLogger.error(errMsg)
      throw new RuntimeException(errMsg)
    }
  }

  def ftpFrom(ftpConfig: FtpConnection, remote: String, local: String): Unit = {
    val ftpClient = getFTPClient(ftpConfig)
    ftpFrom(ftpClient, remote, local)
    closeFTPClient(ftpClient)
  }

  def ftpFrom(ftp: FTPClient, remote: String, local: String): Unit = {
    val localFile = new File(local)
    val outputStream = try {
      new BufferedOutputStream(new FileOutputStream(localFile))
    } catch {
      case e: Exception =>
        ETLLogger.error("Create FileOutputStream failed.", e)
        throw e
    }
    val success = ftp.retrieveFile(
      new String(remote.getBytes(Encoding.UTF8), Encoding.ISO_8859_1),
      outputStream
    )
    if (success) {
      ETLLogger.info(s"Download file success, from FTP '$remote' to local '$local'.")
    } else {
      val errMsg = s"Download file failed, from FTP '$remote' to local '$local'."
      ETLLogger.error(errMsg)
      throw new RuntimeException(errMsg)
    }
    try {
      outputStream.close()
    } catch {
      case e: Exception =>
        ETLLogger.error("Close FileOutputStream failed.", e)
    }
  }

  def load(spark: SparkSession,
           step: WorkflowStep,
           job: JobLog): DataFrame = {
    val sourceConfig = step.getSourceConfig[TextFileDataSourceConfig]
    val prefix = sourceConfig.getConfigPrefix
    val ftpConfig = new FtpConnection(prefix)

    val remoteFilePath = sourceConfig.getFilePath
    val fileName = StringUtil.getFileNameFromPath(remoteFilePath)
    val localTempFilePath = StringUtil.concatFilePath(ftpConfig.getLocalTempDir, fileName)
    val hdfsTempFilePath = CompressionCodecFactory.removeSuffix(
      StringUtil.concatFilePath(ftpConfig.getHdfsTempDir, fileName),
      sourceConfig.codecExtension
    )

    IOUtil.mkdirs(ftpConfig.getLocalTempDir)
    ftpFrom(ftpConfig, remoteFilePath, localTempFilePath)

    HDFSUtil.mkdirs(ftpConfig.getHdfsTempDir)
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

}

object FtpDataSource extends FtpDataSource
