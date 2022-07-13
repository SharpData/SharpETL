package com.github.sharpdata.sharpetl.spark.datasource

import com.github.sharpdata.sharpetl.spark.job.SparkSessionTestWrapper
import com.github.sharpdata.sharpetl.core.datasource.config.{CSVDataSourceConfig, CompressTarConfig}
import com.github.sharpdata.sharpetl.core.util.{HDFSUtil, IOUtil, WorkflowReader}
import org.apache.spark.SparkConf
import org.scalatest.funspec.AnyFunSpec

import java.io.File
import java.nio.file.Files


class CompressTarDataSourceTest extends AnyFunSpec with SparkSessionTestWrapper {
  it("should load data from tar and can be read by follow step ") {
    val conf = new SparkConf().setAppName("CompressTarDataSource").setMaster("local[*]");
    val line = 100

    val files = IOUtil.listFiles("data.tar.gz") ++ IOUtil.listFilesJar("data.tar.gz")
    val rootPath = files.filter(it => it.indexOf("data.tar.gz") != -1).head.replace("/data.tar.gz", "")
    val steps = WorkflowReader.readSteps("compressTar")
    val config = steps.head.getSourceConfig[CompressTarConfig]
    val config2 = steps.last.getSourceConfig[CSVDataSourceConfig]


    val targetPath = s"${rootPath}${config.targetPath}"
    config.setTarPath(s"${rootPath}${config.tarPath}")
    config.setTargetPath(targetPath)
    config.setTmpPath(s"${rootPath}${config.tmpPath}")
    config.setBakPath(s"${rootPath}${config.bakPath}")

    //mock data
    val tesTemplatePath = rootPath
    val dir = new File(targetPath)
    val tmp = new File(config.getTmpPath)
    val bak = new File(config.getBakPath)
    val data1 = new File(s"${targetPath}/data.tar.gz")
    val data2 = new File(s"${targetPath}/data2.tar.gz")

    if (!dir.exists()) dir.mkdirs()
    if (!tmp.exists()) tmp.mkdirs()
    if (!bak.exists()) bak.mkdirs()
    if (data1.exists()) data1.delete()
    if (data2.exists()) data2.delete()

    Files.copy(new File(s"${tesTemplatePath}/data.tar.gz").toPath, data1.toPath)
    Files.copy(new File(s"${tesTemplatePath}/data2.tar.gz").toPath, data2.toPath)


    new CompressTarDataSource().loadFromCompressTar(spark, config)

    config2.setFileDir(s"${rootPath}${config2.fileDir}")
    val file = HDFSUtil.listFileUrl(
      config2.getFileDir,
      config2.getFileNamePattern
    )
    config2.setFilePath(file.head)

    val df2 = new CSVDataSource().loadFromHdfs(spark, config2)
    df2.take(line).foreach(println)
  }
}
