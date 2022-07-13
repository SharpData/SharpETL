package com.github.sharpdata.sharpetl.spark.transformation

import com.fasterxml.jackson.databind.ObjectMapper
import com.github.sharpdata.sharpetl.core.util.{ETLLogger, HDFSUtil}
import com.github.sharpdata.sharpetl.spark.utils.ETLSparkSession
import org.apache.commons.io.{FileUtils, FilenameUtils}
import org.apache.http.HttpResponse
import org.apache.http.client.{ClientProtocolException, ResponseHandler}
import org.apache.http.impl.client._
import org.apache.spark.sql.DataFrame

import java.io.{File, IOException}
import java.nio.file.Paths

// $COVERAGE-OFF$
object HttpDownloadTransformer extends Transformer {
  var httpClient: CloseableHttpClient = _

  lazy val mapper = new ObjectMapper

  override def transform(args: Map[String, String]): DataFrame = {
    downloadFile(args)
    ETLSparkSession.sparkSession.emptyDataFrame
  }

  private def downloadFile(args: Map[String, String]): File = {

    val httpProperties = HttpProperties.initHttpProperties(args)
    val httpRequest = httpProperties.initRequest()
    ETLLogger.info(s"url: ${httpRequest.getURI.toString}")
    if (httpClient == null) {
      httpClient = HttpClients.createDefault()
    }

    val descDirPath = args.getOrElse("tempDestinationDir", "/tmp")
    val sourceFileName = FilenameUtils.getName(httpRequest.getURI.toString)
    val localDescPath = Paths.get(descDirPath, sourceFileName)
    httpClient.execute(httpRequest, new HttpDownloadResponseHandler(localDescPath.toFile))

    if (args.contains("hdfsDir")) {
      val hdfsDir = args.getOrElse("hdfsDir", "/tmp")
      val hdfsDescPath = Paths.get(hdfsDir, sourceFileName).toString
      ETLLogger.info(s"upload the local file ${localDescPath.toString} to hdfs ${hdfsDescPath}")
      HDFSUtil.moveFromLocal(localDescPath.toString, hdfsDescPath)
    }

    localDescPath.toFile
  }

}

class HttpDownloadResponseHandler(val target: File) extends ResponseHandler[File] {
  @throws[ClientProtocolException]
  @throws[IOException]
  def handleResponse(response: HttpResponse): File = {
    val source = response.getEntity.getContent
    FileUtils.copyInputStreamToFile(source, this.target)
    this.target
  }

}

// $COVERAGE-ON$
