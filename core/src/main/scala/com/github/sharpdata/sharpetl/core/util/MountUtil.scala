package com.github.sharpdata.sharpetl.core.util

import com.github.sharpdata.sharpetl.core.datasource.config.RemoteFileDataSourceConfig
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.core.util.Constants.BooleanString
import org.apache.commons.io.FileUtils

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.attribute.PosixFilePermission
import java.nio.file.{Files, StandardCopyOption}
import java.time.ZoneId
import java.util
import java.util.regex.Pattern

object MountUtil {

  def moveFiles(step: WorkflowStep,
                sourceDir: String,
                destinationDir: String,
                timeZone: String,
                startTime: Long,
                endTime: Long,
                permissions: util.Set[PosixFilePermission]): List[String] = {
    val remoteFileDataSourceConfig = step.getSourceConfig[RemoteFileDataSourceConfig]
    new File(sourceDir)
      .listFiles()
      .filter { file =>
        val modifiedTime = Files.getLastModifiedTime(file.toPath).toInstant.atZone(ZoneId.of(timeZone)).toEpochSecond
        file.isFile && remoteFileDataSourceConfig.filterByTime == BooleanString.TRUE && modifiedTime >= startTime && modifiedTime < endTime
      }
      .map { file =>
        val fileName = file.getName
        ETLLogger.info(s"Downloading file: $fileName to $destinationDir")
        val targetFile = new File(StringUtil.concatFilePath(destinationDir, fileName))
        if (remoteFileDataSourceConfig.dos2unix.equals(BooleanString.TRUE)) {
          doc2unix(file, targetFile, permissions)
        } else {
          moveFileToLocal(file, targetFile, permissions)
        }

        fileName
      }
      .toList
  }

  def doc2unix(sourceFile: File, targetFile: File, permissions: util.Set[PosixFilePermission]): Unit = {
    var contents = FileUtils.readFileToString(sourceFile, StandardCharsets.UTF_8)
    contents = Pattern.compile("\r").matcher(contents).replaceAll("")
    FileUtils.write(targetFile, contents, StandardCharsets.UTF_8)
    Files.setPosixFilePermissions(targetFile.toPath, permissions)
  }

  def moveFileToLocal(sourceFile: File, targetFile: File, permissions: util.Set[PosixFilePermission]): Unit = {
    Files.copy(sourceFile.toPath, targetFile.toPath, StandardCopyOption.REPLACE_EXISTING)
    Files.setPosixFilePermissions(targetFile.toPath, permissions)
  }
}
