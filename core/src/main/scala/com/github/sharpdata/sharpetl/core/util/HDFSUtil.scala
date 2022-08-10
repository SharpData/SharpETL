package com.github.sharpdata.sharpetl.core.util

import com.github.sharpdata.sharpetl.core.api.Variables
import com.google.common.base.Strings.isNullOrEmpty
import com.github.sharpdata.sharpetl.core.datasource.config.RemoteFileDataSourceConfig
import com.github.sharpdata.sharpetl.core.exception.Exception.NoFileFoundException
import com.github.sharpdata.sharpetl.core.repository.model.JobLog
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.core.util.Constants.IO_COMPRESSION_CODEC_CLASS.IO_COMPRESSION_CODEC_CLASS_NAMES
import com.github.sharpdata.sharpetl.core.util.Constants.{BooleanString, DataSourceType}
import com.github.sharpdata.sharpetl.core.util.DateUtil.L_YYYY_MM_DD_HH_MM_SS
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.io.compress.{CompressionCodec, CompressionCodecFactory}

import java.io._
import java.nio.file.attribute.PosixFilePermissions
import java.time.{LocalDateTime, ZoneId}
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

// $COVERAGE-OFF$
object HDFSUtil {

  var conf: Configuration = _

  {
    conf = new Configuration()
    conf.setBoolean("fs.hdfs.impl.disable.cache", true)
    conf.setBoolean("dfs.support.append", true)
    conf.set(CommonConfigurationKeys.IO_COMPRESSION_CODECS_KEY, IO_COMPRESSION_CODEC_CLASS_NAMES)
  }

  def getFileSystem(configuration: Configuration = conf): FileSystem = {
    try {
      FileSystem.get(configuration)
    } catch {
      case e: Exception =>
        ETLLogger.error("Init FileSystem failed.", e)
        throw e
    }
  }

  def closeFileSystem(fs: FileSystem): Unit = {
    if (fs != null) {
      try {
        fs.close()
      } catch {
        case e: Exception =>
          ETLLogger.error("Close FileSystem failed.", e)
      }
    }
  }

  def recursiveListFiles(path: String): ListBuffer[String] = {
    val fs = getFileSystem()
    val list = recursiveListFiles(fs, new Path(path))
    closeFileSystem(fs)
    list
  }

  def recursiveListFiles(fs: FileSystem, path: Path): ListBuffer[String] = {
    val paths = new ListBuffer[String]()
    if (fs.exists(path)) {
      if (fs.isFile(path)) {
        paths += path.toString
      } else {
        paths ++= fs
          .listStatus(path)
          .flatMap(fileStatus => recursiveListFiles(fs, fileStatus.getPath))
      }
    }
    paths
  }

  def listFileStatus(path: String): Seq[FileStatus] = {
    val fs = getFileSystem()
    val list = fs.listStatus(new Path(path))
    closeFileSystem(fs)
    list
  }

  def extractFileName(path: String): String = {
    val slash = path.lastIndexOf(Path.SEPARATOR)
    path.substring(slash + 1)
  }

  def readFile(filePath: String, fs: FileSystem = getFileSystem()): InputStream = {
    fs.open(new Path(filePath))
  }

  def getBytesDataReader(path: String): DataInputStream = {
    new DataInputStream(readFile(path))
  }

  def readLines(path: String): List[String] = {
    val fs = getFileSystem()
    val lines = readLines(fs, new Path(path))
    closeFileSystem(fs)
    lines
  }

  def readLines(fs: FileSystem, path: Path): List[String] = {
    val bufferedReader = try {
      new BufferedReader(new InputStreamReader(fs.open(path)))
    } catch {
      case e: IOException =>
        ETLLogger.error(s"Open InputStreamReader with path $path failed.", e)
        throw e
    }

    val lines = new ArrayBuffer[String]()
    // scalastyle:off
    var line: String = null
    // scalastyle:on
    while ( {
      line = bufferedReader.readLine();
      Option(line).isDefined
    }) {
      lines += line
    }

    try {
      bufferedReader.close()
    } catch {
      case e: IOException =>
        ETLLogger.error("Close BufferedReader failed.", e)
    }
    lines.toList
  }

  def listFileUrl(dir: String, fileNamePattern: String): List[String] = {
    val fs = getFileSystem()
    val list = listFileUrl(fs, dir, fileNamePattern)
    closeFileSystem(fs)
    list
  }

  def listFileUrl(fs: FileSystem, dir: String, fileNamePattern: String): List[String] = {
    val dirPath = new Path(dir)
    if (fs.exists(dirPath)) {
      fs
        .listStatus(
          dirPath,
          new PathFilter {
            override def accept(
                                 path: Path): Boolean = fileNamePattern.r.findFirstMatchIn(path.getName).isDefined
          }
        )
        .map(_.getPath.toString)
        .toList
    } else {
      List[String]()
    }
  }

  def exists(path: String): Boolean = {
    val fs = getFileSystem()
    fs.exists(new Path(path))
  }

  def delete(path: String, recursive: Boolean = true): Unit = {
    val fs = getFileSystem()
    delete(fs, path, recursive)
    closeFileSystem(fs)
  }

  def delete(fs: FileSystem, path: String, recursive: Boolean): Unit = {
    delete(fs, new Path(path), recursive)
  }

  def delete(fs: FileSystem, path: Path, recursive: Boolean): Unit = {
    if (fs.exists(path)) {
      fs.delete(path, recursive)
      ETLLogger.info(s"Delete file '$path' success.")
    } else {
      ETLLogger.info(s"File '$path' not exists.")
    }
  }

  def mkdirs(dir: String): Unit = {
    val fs = getFileSystem()
    mkdirs(fs, dir)
    closeFileSystem(fs)
  }

  def mkdirs(fs: FileSystem, dir: String): Unit = {
    fs.mkdirs(new Path(dir))
  }

  def mv(src: String, target: String, overWrite: Boolean): Unit = {
    val fs = getFileSystem()
    mv(fs, src, target, overWrite)
    closeFileSystem(fs)
  }

  def mv(fs: FileSystem, src: String, target: String, overWrite: Boolean): Unit = {
    mv(fs, new Path(src), new Path(target), overWrite)
  }

  def mv(fs: FileSystem, src: Path, target: Path, overWrite: Boolean): Unit = {
    if (overWrite) {
      delete(fs, target, recursive = true)
    }
    val mvResult = try {
      fs.rename(src, target)
    } catch {
      case e: FileAlreadyExistsException =>
        ETLLogger.error(s"Rename file '$src' to '$target' failed, target path '$target' has already exists.")
        throw e
      case e: Exception =>
        ETLLogger.error(e.getMessage)
        throw e
    }
    if (mvResult) {
      ETLLogger.info(s"Rename file '$src' to '$target' success.")
    } else {
      throw new RuntimeException(s"Rename file '$src' to '$target' failed.")
    }
  }


  def put(src: String, dst: String): Unit = {
    put(src, dst, "", decompress = false)
  }

  def put(
           src: String,
           dst: String,
           extension: String,
           decompress: Boolean): Unit = {
    val fs = getFileSystem()
    put(fs, src, dst, extension, decompress)
    closeFileSystem(fs)
  }

  def put(
           fs: FileSystem,
           src: String,
           dst: String,
           extension: String,
           decompress: Boolean): Unit = {
    val fileInputStream = new FileInputStream(src)
    val codec = getCodecByExtension(extension)
    val in = if (decompress && codec.isDefined) {
      codec.get.createInputStream(new FileInputStream(src))
    } else {
      fileInputStream
    }
    ETLLogger.info(s"put local file '$src' to HDFS '$dst'")
    val out = fs.create(new Path(dst))
    IOUtils.copyBytes(in, out, conf, true)
  }

  def getCodecByExtension(codecExtension: String): Option[CompressionCodec] = {
    val factory = new CompressionCodecFactory(conf)
    val codecClassName = CodecUtil.matchCodec(codecExtension)
    if (codecClassName.isDefined) {
      Some(factory.getCodecByClassName(codecClassName.get))
    } else {
      None
    }
  }

  def moveFromLocal(src: String, des: String): Unit = {
    val fs = getFileSystem()
    moveFromLocal(fs, src, des)
  }

  def append(dst: String, content: String): Unit = {
    val fs = getFileSystem()
    val path = new Path(dst)
    val out: FSDataOutputStream = fs.append(path)
    out.writeBytes(content)
    out.hflush()
    out.close()
  }

  def moveFromLocal(fs: FileSystem, src: String, des: String): Unit = {
    ETLLogger.info(s"Uploading from local path $src to HDFS path $des...")
    fs.moveFromLocalFile(new Path(src), new Path(des))
  }

  def downloadFileToHDFS(step: WorkflowStep,
                         jobLog: JobLog,
                         variables: Variables): List[String] = {
    downloadRemoteFilesToLocal(step, jobLog, variables)

    val dataSourceConfig = step.getSourceConfig[RemoteFileDataSourceConfig]
    val hdfsPaths = ListBuffer[String]()
    if (!isNullOrEmpty(jobLog.file)) {
      jobLog.file.split(",")
        .foreach(fileName => {
          val hdfsFilePath = StringUtil.concatFilePath(dataSourceConfig.hdfsDir, fileName)
          HDFSUtil.moveFromLocal(
            StringUtil.concatFilePath(dataSourceConfig.tempDestinationDir, fileName),
            hdfsFilePath)
          hdfsPaths += hdfsFilePath
        }
        )
      ETLLogger.info(s"Uploaded file to HDFS ${hdfsPaths.mkString(",")}")
    }
    hdfsPaths.toList
  }

  def downloadRemoteFilesToLocal(step: WorkflowStep,
                                 jobLog: JobLog,
                                 variables: Variables): Unit = {
    val dataSourceConfig = step.getSourceConfig[RemoteFileDataSourceConfig]
    val startTime = LocalDateTime.parse(variables("${DATA_RANGE_START}"), L_YYYY_MM_DD_HH_MM_SS).atZone(ZoneId.of(dataSourceConfig.timeZone)).toEpochSecond
    val endTime = LocalDateTime.parse(variables("${DATA_RANGE_END}"), L_YYYY_MM_DD_HH_MM_SS).atZone(ZoneId.of(dataSourceConfig.timeZone)).toEpochSecond
    val permission = step.getSourceConfig[RemoteFileDataSourceConfig].tempDestinationDirPermission
    val permissions = PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString(permission)).value()

    if (!new File(dataSourceConfig.tempDestinationDir).exists()) {
      new File(dataSourceConfig.tempDestinationDir).mkdirs()
    }
    val fileNames = dataSourceConfig.getDataSourceType match {
      case DataSourceType.SFTP =>
        SFTPUtil.downloadFiles(step, dataSourceConfig.configPrefix, dataSourceConfig.sourceDir,
          dataSourceConfig.tempDestinationDir, startTime, endTime, permissions)
      case DataSourceType.MOUNT =>
        MountUtil.moveFiles(step, dataSourceConfig.sourceDir, dataSourceConfig.tempDestinationDir,
          dataSourceConfig.timeZone, startTime, endTime, permissions)
      case _ => ???
    }
    if (fileNames == null || fileNames.isEmpty) {
      if (dataSourceConfig.breakFollowStepWhenEmpty == BooleanString.TRUE) {
        throw NoFileFoundException(step.step)
      }
      ETLLogger.warn("No files need download, and current config `breakFollowStepWhenEmpty` is false, so the job will continue the next steps.")
    } else {
      jobLog.file = fileNames.mkString(",")
      ETLLogger.info("Downloaded file to local")
    }
  }
}
// $COVERAGE-ON$
