package com.github.sharpdata.sharpetl.spark.transformation

import com.github.sharpdata.sharpetl.core.util.DateUtil.L_YYYY_MM_DD_HH_MM_SS
import com.github.sharpdata.sharpetl.core.util.HDFSUtil.extractFileName
import com.github.sharpdata.sharpetl.core.util.{ETLLogger, HDFSUtil}
import com.github.sharpdata.sharpetl.spark.utils.ETLSparkSession
import org.apache.spark.sql

import java.io.FileOutputStream
import java.time.{LocalDateTime, ZoneId}
import java.util.zip.{ZipEntry, ZipOutputStream}
import scala.collection.mutable

// $COVERAGE-OFF$
/**
 * usage:
 * 1. for simple file name and date range:
 * -- step=1
 * -- source
 * --  dataSourceType=transformation
 * --  className=com.github.sharpdata.sharpetl.spark.transformation.ZipFilesTransformer
 * --  transformerType=object
 * --  methodName=transform
 * --  fileBasePath=hdfs:///data/test
 * --  fileStart=test-20220503000000.txt
 * --  fileEnd=test-20220603000000.txt
 * --  zipFilePath=hdfs:///data/test
 * --  zipFileName=test-202206.zip
 * -- target
 * --  dataSourceType=do_nothing
 * 2. for multiple files compress to multiple zip files:
 * -- step=1
 * -- source
 * --  dataSourceType=transformation
 * --  className=com.github.sharpdata.sharpetl.spark.transformation.ZipFilesTransformer
 * --  transformerType=object
 * --  methodName=transform
 * --  fileBasePath=hdfs:///data/test
 * --  fileStart=test-20220503000000.txt,ships_20220528.txt
 * --  fileEnd=test-20220603000000.txt,ships_20220530.txt
 * --  zipFilePath=hdfs:///data/test
 * --  zipFileName=test-202206.zip,ships.zip
 * -- target
 * --  dataSourceType=do_nothing
 * 3. for multiple files compress to one single zip files:
 * -- step=1
 * -- source
 * --  dataSourceType=transformation
 * --  className=com.github.sharpdata.sharpetl.spark.transformation.ZipFilesTransformer
 * --  transformerType=object
 * --  methodName=transform
 * --  fileBasePath=hdfs:///data/test
 * --  fileStart=test-20220503000000.txt,ships_20220528.txt
 * --  fileEnd=test-20220603000000.txt,ships_20220530.txt
 * --  zipFilePath=hdfs:///data/test
 * --  zipFileName=results.zip
 * -- target
 * --  dataSourceType=do_nothing
 * 4. compress file by file name pattern regex and modified time:
 * -- step=1
 * -- source
 * --  dataSourceType=transformation
 * --  className=com.github.sharpdata.sharpetl.spark.transformation.ZipFilesTransformer
 * --  transformerType=object
 * --  methodName=transform
 * --  fileBasePath=hdfs:///data/test
 * --  zipFilePath=hdfs:///data/test
 * --  zipFileName=results.zip
 * --  filterByLastModified=true
 * --  fileNamePattern=New Area - file_name_\d\d-\w\w\w-\d\d\d\d.zip
 * --  fileStart=2022-06-10 00:00:00
 * --  fileEnd=2022-06-11 00:00:00
 * -- target
 * --  dataSourceType=do_nothing
 */
object ZipFilesTransformer extends Transformer {
  val BUFFER_SIZE = 1024
  var gfos: FileOutputStream = _
  var gzipOut: ZipOutputStream = _
  val fileBuffer: mutable.Buffer[String] = mutable.Buffer.empty

  override def transform(args: Map[String, String]): sql.DataFrame = {
    gfos = null // scalastyle:off
    gzipOut = null // scalastyle:off
    fileBuffer.clear()
    val fileBasePath = args("fileBasePath")
    val fileStart = args("fileStart") // include
    val fileEnd = args("fileEnd") // exclude
    val zipFilePath = args("zipFilePath")
    val zipFileName = args("zipFileName")
    val filterByLastModified = args.getOrElse("filterByLastModified", "false").toBoolean
    val fileNamePattern = args("fileNamePattern")

    val zipFiles = zipFileName.split(",")
    val sameZip = zipFiles.size == 1

    if (filterByLastModified) {
      // hard-code timezone to Z which means UTC timezone, because [[file.getModificationTime]] returns milliseconds since January 1, 1970 UTC.
      val startTime = LocalDateTime.parse(fileStart, L_YYYY_MM_DD_HH_MM_SS).atZone(ZoneId.of("Z")).toEpochSecond * 1000
      val endTime = LocalDateTime.parse(fileEnd, L_YYYY_MM_DD_HH_MM_SS).atZone(ZoneId.of("Z")).toEpochSecond * 1000
      val fileLists = HDFSUtil.listFileStatus(fileBasePath)
        .filter { file =>
          file.getPath.getName.matches(fileNamePattern) &&
            file.getModificationTime >= startTime &&
            file.getModificationTime < endTime
        }
        .map(_.getPath.getName)
        .toSeq
      processCompress(fileBasePath, zipFileName, zipFiles, sameZip, fileStart, fileEnd, 0, fileLists)
    } else {
      fileStart.split(",")
        .zip(fileEnd.split(","))
        .zipWithIndex
        .foreach {
          case ((fileStart: String, fileEnd: String), idx) =>
            val fileLists = HDFSUtil.recursiveListFiles(fileBasePath)
              .map(extractFileName)
              .filter(file => file.length == fileStart.length && file >= fileStart && file < fileEnd)
              .toSeq
            processCompress(fileBasePath, zipFileName, zipFiles, sameZip, fileStart, fileEnd, idx, fileLists)
        }
    }

    if (sameZip) {
      gzipOut.close()
      gfos.close()
      uploadFileAndDeleteLocalFile(zipFilePath, zipFileName)
    } else {
      zipFiles.foreach(zip => {
        uploadFileAndDeleteLocalFile(zipFilePath, zip)
      })
    }

    fileBuffer.foreach { file =>
      ETLLogger.warn(s"Deleting file from HDFS $fileBasePath/$file")
      HDFSUtil.delete(s"$fileBasePath/$file", false)
    }

    ETLSparkSession.sparkSession.emptyDataFrame
  }

  private def processCompress(fileBasePath: String, zipFileName: String, zipfiles: Array[String], sameZip: Boolean,
                              fileStart: String, fileEnd: String, idx: Int, fileLists: Seq[String]) = {
    ETLLogger.warn(s"Files will be zipped and deleted by [$fileStart, $fileEnd): \n ${fileLists.mkString(",\n")}")

    val (fos: FileOutputStream, zipOut: ZipOutputStream) = createZipStream(if (sameZip) zipFileName else zipfiles(idx), sameZip)
    fileLists.foreach(file => {
      fileBuffer.append(file)
      val fis = HDFSUtil.readFile(s"$fileBasePath/$file")
      val zipEntry = new ZipEntry(file)
      zipOut.putNextEntry(zipEntry)

      val bytes = new Array[Byte](BUFFER_SIZE)
      var length = 0

      while (length >= 0) {
        length = fis.read(bytes)
        if (length > 0) {
          zipOut.write(bytes, 0, length)
        }
      }
      fis.close()
    })

    if (!sameZip) {
      zipOut.close()
      fos.close()
    }
  }

  private def uploadFileAndDeleteLocalFile(zipFilePath: String, zip: String) = {
    ETLLogger.info(s"Uploading zip file $zip to HDFS $zipFilePath/$zip...")
    HDFSUtil.moveFromLocal(zip, s"$zipFilePath/$zip")
  }

  private def createZipStream(zipFileName: String, sameZip: Boolean) = {
    if (!sameZip) {
      val fos = new FileOutputStream(zipFileName)
      val zipOut = new ZipOutputStream(fos)
      (fos, zipOut)
    } else {
      if (gfos == null && gzipOut == null) {
        val fos = new FileOutputStream(zipFileName)
        val zipOut = new ZipOutputStream(fos)
        gfos = fos
        gzipOut = zipOut
        (gfos, gzipOut)
      } else {
        (gfos, gzipOut)
      }
    }

  }
}
// $COVERAGE-ON$
