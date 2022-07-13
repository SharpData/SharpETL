package com.github.sharpdata.sharpetl.core.util

import com.github.sharpdata.sharpetl.core.datasource.config.RemoteFileDataSourceConfig
import com.github.sharpdata.sharpetl.core.datasource.connection.SftpConnection
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.core.util.Constants.BooleanString
import com.jcraft.jsch.ChannelSftp

import java.nio.file.attribute.PosixFilePermission
import java.util

object SFTPUtil {

  def downloadFiles(step: WorkflowStep,
                    prefix: String,
                    sourceDir: String,
                    destinationDir: String,
                    startTime: Long,
                    endTime: Long,
                    permissions: util.Set[PosixFilePermission]): List[String] = {
    val configuration = new SftpConnection(prefix)
    val sFTPClient = new SFTPClient(configuration)
    val remoteFileDataSourceConfig = step.getSourceConfig[RemoteFileDataSourceConfig]

    val filter = (entry: ChannelSftp#LsEntry) => {
      if (remoteFileDataSourceConfig.filterByTime == BooleanString.TRUE) {
        entry.getFilename.matches(remoteFileDataSourceConfig.fileNamePattern) &&
          entry.getAttrs.getMTime >= startTime &&
          entry.getAttrs.getMTime < endTime
      } else {
        entry.getFilename.matches(remoteFileDataSourceConfig.fileNamePattern)
      }
    }
    val fileNames = sFTPClient.listFiles(sourceDir, filter)
    ETLLogger.info(s"fileNames is ${fileNames.mkString(",")}")
    fileNames.foreach(fileName => sFTPClient.downloadFileToLocal(fileName, sourceDir, destinationDir, permissions))
    sFTPClient.close()
    fileNames
  }

}
