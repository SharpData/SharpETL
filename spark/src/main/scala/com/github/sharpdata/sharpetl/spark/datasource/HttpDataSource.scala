package com.github.sharpdata.sharpetl.spark.datasource

import com.fasterxml.jackson.databind.ObjectMapper
import com.github.sharpdata.sharpetl.core.annotation.source
import com.github.sharpdata.sharpetl.core.api.Variables
import com.github.sharpdata.sharpetl.core.datasource.Source
import com.github.sharpdata.sharpetl.core.datasource.config.HttpDataSourceConfig
import com.github.sharpdata.sharpetl.core.repository.model.JobLog
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.core.util.{ETLConfig, ETLLogger}
import com.github.sharpdata.sharpetl.spark.utils.{ETLSparkSession, HttpStatusUtils}
import com.google.common.base.Strings.isNullOrEmpty
import com.jayway.jsonpath.JsonPath
import net.minidev.json.JSONArray
import org.apache.http.HttpHost
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{HttpGet, HttpPost, HttpRequestBase}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client._
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.net.URLEncoder
import scala.util.Using

@source(types = Array("http"))
class HttpDataSource extends Source[DataFrame, SparkSession] {

  var httpClient: CloseableHttpClient = _

  lazy val mapper = new ObjectMapper

  def read(step: WorkflowStep, jobLog: JobLog, executionContext: SparkSession, variables: Variables): DataFrame = {
    val config = step.getSourceConfig[HttpDataSourceConfig]
    val responseBody = getHttpResponseBody(config)
    transformerDF(responseBody, config)
  }

  private def getHttpResponseBody(config: HttpDataSourceConfig): String = {

    val httpProperties = HttpProperties.initHttpProperties(config)
    val httpRequest = httpProperties.initRequest()
    ETLLogger.info(s"url: ${httpRequest.getURI.toString}")
    if (httpClient == null) {
      httpClient = HttpClients.createDefault()
    }
    Using(httpClient.execute(httpRequest)) { response =>
      if (!HttpStatusUtils.isSuccessful(response.getStatusLine)) {
        throw new HttpBadRequestException(s"${httpRequest.getURI.toString} failed with statusCode ${response.getStatusLine.getStatusCode}")
      }
      val resp = EntityUtils.toString(response.getEntity)
      val LOG_MAX_LENGTH = 10000
      ETLLogger.info(s"response length: ${resp.length}")
      ETLLogger.info(if (resp.length > LOG_MAX_LENGTH) {
        resp.substring(0, LOG_MAX_LENGTH)
      } else {
        resp
      })
      resp
    }.get
  }

  private def transformerDF(value: String, config: HttpDataSourceConfig): DataFrame = {
    val fieldName = config.fieldName
    val jsonPath = config.jsonPath
    val splitBy = config.splitBy

    var result = value
    if (jsonPath != "$" || splitBy != "") {
      val values = JsonPath.read[Object](value, jsonPath)
      result = values match {
        case jsonArray: JSONArray =>
          if (splitBy != "") {
            jsonArray.toArray()
              .map {
                case s: String => s
                case o: Any => mapper.writeValueAsString(o)
              }
              .mkString(splitBy)
          } else {
            jsonArray.toJSONString()
          }
        case _ =>
          if (jsonPath != "$") {
            mapper.writeValueAsString(values)
          } else {
            value
          }
      }
    }

    val row = Row(result)
    val schema = StructType(List(StructField(fieldName, StringType, nullable = false)))
    ETLSparkSession
      .sparkSession
      .createDataFrame(ETLSparkSession.sparkSession.sparkContext.parallelize(Seq(row)), schema)
  }

}

object HttpProperties {

  def getEncodeUrl(url: String): String = {
    if (url == null) {
      throw new HttpBadVariableException("HttpRequest url can not be null")
    }
    if (!url.contains("?")) url else handleUrl(url)
  }

  private def handleUrl(url: String): String = {
    val arguments = url.split("\\?")
    val (baseUrl, requestParameters) = (arguments(0) + "?", arguments(1))
    val params = requestParameters.split("&")
    baseUrl + params.map(encodeParams).mkString("&")
  }

  private def encodeParams(it: String): String = {
    val keyAndValue = it.split("=")
    s"""${URLEncoder.encode(keyAndValue(0), "UTF-8")}=${URLEncoder.encode(keyAndValue(1), "UTF-8")}"""
  }

  def initHttpProperties(config: HttpDataSourceConfig): HttpProperties = {
    val url = getEncodeUrl(config.url)
    val httpMethod = config.httpMethod
    val connectionName = config.connectionName
    if (!isNullOrEmpty(connectionName)) {
      val httpConnectionProperties = ETLConfig.getHttpProperties(connectionName)
      val headers = httpConnectionProperties
        .filter(_._1.startsWith("header."))
        .map { case (key, value) => key.substring("header.".length, key.length) -> value }

      val proxyProperties = httpConnectionProperties
        .filter(_._1.startsWith("proxy."))
        .map { case (key, value) => key.substring("proxy.".length, key.length) -> value }
      val proxyHost = proxyProperties.get("host")
      if (proxyHost.isEmpty) {
        throw new HttpBadVariableException("Http proxy host can not be null")
      }
      val proxyPort = proxyProperties.getOrElse("port", "8080").toInt
      val proxy = new HttpHost(proxyHost.get, proxyPort)
      new HttpProperties(url, httpMethod, headers, Option(proxy), config)
    } else {
      new HttpProperties(url, httpMethod, Map.empty, Option.empty, config)
    }
  }
}

class HttpProperties(url: String, httpMethod: String, headers: Map[String, String], proxy: Option[HttpHost], config: HttpDataSourceConfig) {

  def initRequest(): HttpRequestBase = {
    val httpRequest = httpMethod.toUpperCase match {
      case "GET" => new HttpGet(url)
      case _ =>
        val httpPost = new HttpPost(url)
        if (!isNullOrEmpty(config.requestBody)) {
          val requestBody = config.requestBody
          ETLLogger.info(s"request the $url with the body $requestBody")
          httpPost.setEntity(new StringEntity(requestBody))
          httpPost.addHeader("Content-type", "application/json")
        }
        httpPost
    }
    if (headers.nonEmpty) {
      headers.foreach((entry: (String, String)) => httpRequest.addHeader(entry._1, entry._2))
    }
    if (proxy.isDefined) {
      httpRequest.setConfig(RequestConfig.custom().setProxy(proxy.get).build())
    }
    httpRequest
  }
}

class HttpBadVariableException(message: String) extends RuntimeException(message)

class HttpBadRequestException(message: String) extends RuntimeException(message)
