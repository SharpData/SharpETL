package com.github.sharpdata.sharpetl.core.util


import com.aliyun.oss.OSSClientBuilder
import com.github.sharpdata.sharpetl.core.util.HDFSUtil.readInputStreamInToLines

import java.io.InputStream
import scala.jdk.CollectionConverters._

object OssUtil {

  lazy val regex = "oss://([^/]*)/(.*)".r
  lazy val accessKeyId = System.getenv("PROP_AK")
  lazy val accessKeySecret = System.getenv("PROP_SK")
  lazy val endpoint = System.getenv("PROP_ENDPOINT")
  lazy val ossClient = new OSSClientBuilder()
    .build(endpoint, accessKeyId, accessKeySecret)

  def recursiveListFiles(configRootDir: String): List[String] = {
    val regex(bucketName, prefix) = configRootDir
    ossClient.listObjectsV2(bucketName, prefix)
      .getObjectSummaries
      .asScala
      .map(it => s"oss://$bucketName/${it.getKey}")
      .filter(it => it.endsWith("sql") || it.endsWith("scala"))
      .toList
  }

  def readLines(taskPath: String): List[String] = {
    val regex(bucketName, key) = taskPath

    val in = ossClient
      .getObject(bucketName, key)
      .getObjectContent
    readInputStreamInToLines(in, taskPath)
  }

  def readFile(propertyPath: String): InputStream = {
    val regex(bucketName, key) = propertyPath

    ossClient
      .getObject(bucketName, key)
      .getObjectContent
  }
}
