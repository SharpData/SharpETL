package com.github.sharpdata.sharpetl.spark.datasource

import com.github.sharpdata.sharpetl.core.api.Variables
import com.github.sharpdata.sharpetl.core.datasource.Sink
import com.github.sharpdata.sharpetl.core.datasource.config._
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.core.util.Constants.{DataSourceType, TransformerType}
import com.github.sharpdata.sharpetl.core.util.HDFSUtil._
import com.github.sharpdata.sharpetl.core.util.{ETLConfig, HDFSUtil, StringUtil}
import com.github.sharpdata.sharpetl.core.annotation._
import com.github.sharpdata.sharpetl.spark.utils.ReflectUtil
import org.apache.spark.sql.{DataFrame, Row}

@sink(types = Array("hdfs"))
class HdfsDataSource extends Sink[DataFrame] {

  override def sink(df: DataFrame, step: WorkflowStep, variables: Variables): Unit = {
    save(df, step)
  }

  def filter(files: List[String], step: WorkflowStep): List[String] = {
    val sourceConfig = step.getSourceConfig[FileDataSourceConfig]
    val func = sourceConfig.getFileFilterFunc

    val result = ReflectUtil.execute(func, "filter", TransformerType.OBJECT_TYPE, files)
    result.asInstanceOf[List[String]]
  }

  def listFileUrl(step: WorkflowStep): List[String] = {
    val sourceConfig = step.getSourceConfig[FileDataSourceConfig]
    var dir = ETLConfig.getProperty(s"${sourceConfig.getConfigPrefix}.hdfs.dir")
    if (sourceConfig.getFileDir != null && sourceConfig.getFileDir != "") {
      dir = sourceConfig.getFileDir
    }
    val files = HDFSUtil.listFileUrl(
      dir,
      sourceConfig.getFileNamePattern
    )
    if (sourceConfig.getFileFilterFunc != null) {
      filter(files, step)
    } else {
      files
    }
  }

  def getTargetDir(step: WorkflowStep): String = {
    val targetConfig = step.getTargetConfig[TextFileDataSourceConfig]
    if (targetConfig.getFileDir != null && targetConfig.getFileDir != "") {
      targetConfig.getFileDir
    } else {
      ETLConfig.getProperty(s"${targetConfig.getConfigPrefix}.hdfs.dir")
    }
  }

  private def saveFromFile(step: WorkflowStep): Unit = {
    val targetDir = getTargetDir(step)
    mkdirs(targetDir)
    val src = step.getSourceConfig[TextFileDataSourceConfig].getFilePath
    val fileName = StringUtil.getFileNameFromPath(src)
    val target = StringUtil.concatFilePath(targetDir, fileName)
    mv(src, target, overWrite = true)
  }

  private def saveFromDB(df: DataFrame, step: WorkflowStep): Unit = {
    val targetConfig = step.getTargetConfig[TextFileDataSourceConfig]
    val targetPath = targetConfig.filePath
    val encoding = targetConfig.getEncoding
    val codecExtension = targetConfig.getCodecExtension
    val separator = targetConfig.getSeparator
    val mapFunction = if (separator != null & separator != "") {
      (row: Row) => row.toSeq.mkString(separator)
    } else {
      val fieldLengthArray = targetConfig
        .getFieldLengthConfig
        .split(",")
        .map(_.toInt)
        .zipWithIndex
      (row: Row) => {
        fieldLengthArray
          .map {
            case (fieldLength, index) =>
              val result = Array.fill(fieldLength)(32.toByte)
              val fieldValue = row.get(index).toString
              fieldValue.getBytes(encoding).copyToArray(result)
              result
          }
          .mkString
      }
    }
    val codec = getCodecByExtension(codecExtension)
    val rdd = df
      .rdd
      .map(mapFunction)
      .repartition(1)
    val tempTargetPath = StringUtil.uuid
    if (codec.isDefined) {
      rdd.saveAsTextFile(tempTargetPath, codec.get.getClass)
    } else {
      rdd.saveAsTextFile(tempTargetPath)
    }
    mv(
      HDFSUtil.listFileUrl(tempTargetPath, "part-00000.*").head,
      targetPath,
      overWrite = true
    )
    delete(tempTargetPath)
  }

  def save(df: DataFrame, step: WorkflowStep): Unit = {
    step.source.dataSourceType match {
      case DataSourceType.SCP | DataSourceType.FTP | DataSourceType.HDFS =>
        saveFromFile(step)
      case _ =>
        saveFromDB(df, step)
    }
  }
}

object HdfsDataSource extends HdfsDataSource
