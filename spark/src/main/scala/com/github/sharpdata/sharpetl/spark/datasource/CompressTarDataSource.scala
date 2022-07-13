package com.github.sharpdata.sharpetl.spark.datasource

import com.github.sharpdata.sharpetl.core.api.Variables
import com.github.sharpdata.sharpetl.core.datasource.Source
import java.io.{File, FileOutputStream}
import com.github.sharpdata.sharpetl.core.datasource.config.CompressTarConfig
import com.github.sharpdata.sharpetl.core.exception.Exception.StepFailedException
import com.github.sharpdata.sharpetl.core.repository.model.JobLog
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.core.util.{ETLLogger, HDFSUtil}
import com.github.sharpdata.sharpetl.core.annotation._
import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveInputStream}
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.matching.Regex


@source(types = Array("compresstar"))
class CompressTarDataSource extends Source[DataFrame, SparkSession] {

  val replaceCharOld = "/"
  val replaceCharNew = "_"
  val bufferSize = 4096

  override def read(step: WorkflowStep, jobLog: JobLog, executionContext: SparkSession, variables: Variables): DataFrame = {
    loadFromCompressTar(executionContext, step.getSourceConfig)
  }

  def loadFromCompressTar(spark: SparkSession, dataSourceConfig: CompressTarConfig): DataFrame = {
    val filesPath = dataSourceConfig.getTarPath
    val tarDir = filesPath.substring(0, filesPath.lastIndexOf(replaceCharOld))
    val tarPattern = filesPath.substring(filesPath.lastIndexOf(replaceCharOld) + 1, filesPath.length)

    HDFSUtil.listFileUrl(tarDir, tarPattern)
      .map(it => it.substring(it.lastIndexOf(replaceCharOld) + 1, it.length))
      .map(it => s"${tarDir}/${it}")
      .foreach(it => {
        extractFile(dataSourceConfig, it)
      })
    spark.emptyDataFrame
  }

  private def filterByFilePatten(sourceFile: String, filePattern: Regex): Boolean = {
    filePattern findFirstMatchIn sourceFile match {
      case Some(_) => true
      case _ => false
    }
  }


  private def getTargetFileName(targetPath: String, fileName: String): String = {
    val targetFile = fileName.replace(replaceCharOld, replaceCharNew)
    targetPath + targetFile
  }

  private def getBaktFileName(bakPath: String, fileName: String): String = {
    val bakFileName = fileName.substring(fileName.lastIndexOf("/") + 1)
    bakPath + bakFileName
  }

  private def getTmpFileName(targetPath: String, fileName: String): String = {
    val targetFile = fileName.replace(replaceCharOld, replaceCharNew)
    targetPath + targetFile
  }


  def isHDFSPath(path: String): Boolean = {
    path.indexOf("hdfs:") != -1
  }

  private def isPassEmptyFile(config: CompressTarConfig, entry: TarArchiveEntry): Boolean = {
    config.isPassEmptyFile == true.toString && entry.getSize == 0L
  }

  private def extractFile(config: CompressTarConfig, file: String): Unit = {
    val filePatten = new Regex(config.fileNamePattern)
    try {
      val fs = HDFSUtil.getFileSystem()
      val tar = new TarArchiveInputStream(new GzipCompressorInputStream(HDFSUtil.readFile(file, fs)))

      var entry: TarArchiveEntry = tar.getNextTarEntry
      while (entry != null) {
        if (!entry.isDirectory && filterByFilePatten(entry.getName, filePatten) && !isPassEmptyFile(config, entry)) {
          ETLLogger.info(s"exact fileName:${entry.getName}")

          val targetFileName = getTargetFileName(config.getTargetPath, entry.getName)
          val tmpFileName = getTmpFileName(config.tmpPath, entry.getName)
          val output = new FileOutputStream(new File(tmpFileName), false)

          val buffer = new Array[Byte](bufferSize)
          var len = tar.read(buffer)
          while (len != -1) {
            output.write(buffer, 0, len)
            len = tar.read(buffer)
          }
          output.flush()
          fs.moveFromLocalFile(new Path(tmpFileName), new Path(targetFileName))
        }
        entry = tar.getNextTarEntry
      }
      HDFSUtil.mv(fs, file, getBaktFileName(config.bakPath, file), true)
      tar.close()
      HDFSUtil.closeFileSystem(fs)
    } catch {
      case e: Exception => throw StepFailedException(e, "appear exception when unzip tar !")

    }
  }
}
