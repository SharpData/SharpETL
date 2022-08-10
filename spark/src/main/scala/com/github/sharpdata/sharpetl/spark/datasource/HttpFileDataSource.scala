package com.github.sharpdata.sharpetl.spark.datasource

import com.github.sharpdata.sharpetl.core.annotation.source
import com.github.sharpdata.sharpetl.core.api.Variables
import com.github.sharpdata.sharpetl.core.datasource.Source
import com.github.sharpdata.sharpetl.core.datasource.config.HttpFileDataSourceConfig
import com.github.sharpdata.sharpetl.core.repository.model.JobLog
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.core.util.{ETLLogger, HDFSUtil}
import com.github.sharpdata.sharpetl.spark.utils.ETLSparkSession
import org.apache.commons.io.{FileUtils, FilenameUtils}
import org.apache.http.HttpResponse
import org.apache.http.client.{ClientProtocolException, ResponseHandler}
import org.apache.http.impl.client._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.{File, IOException}
import java.nio.file.Paths

// $COVERAGE-OFF$
@source(types = Array("http_file"))
class HttpFileDataSource extends Source[DataFrame, SparkSession] {
  var httpClient: CloseableHttpClient = _

  def read(step: WorkflowStep, jobLog: JobLog, executionContext: SparkSession, variables: Variables): DataFrame = {
    val config = step.getSourceConfig[HttpFileDataSourceConfig]
    downloadFile(config)
    ETLSparkSession.sparkSession.emptyDataFrame
  }


  private def downloadFile(config: HttpFileDataSourceConfig): File = {

    val httpProperties = HttpProperties.initHttpProperties(config)
    val httpRequest = httpProperties.initRequest()
    ETLLogger.info(s"url: ${httpRequest.getURI.toString}")
    if (httpClient == null) {
      httpClient = HttpClients.createDefault()
    }

    val descDirPath = config.tempDestinationDir
    val sourceFileName = FilenameUtils.getName(httpRequest.getURI.toString)
    val localDescPath = Paths.get(descDirPath, sourceFileName)
    httpClient.execute(httpRequest, new HttpDownloadResponseHandler(localDescPath.toFile))

    val hdfsDir = config.hdfsDir
    val hdfsDescPath = Paths.get(hdfsDir, sourceFileName).toString
    ETLLogger.info(s"upload the local file ${localDescPath.toString} to hdfs ${hdfsDescPath}")
    HDFSUtil.moveFromLocal(localDescPath.toString, hdfsDescPath)

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
