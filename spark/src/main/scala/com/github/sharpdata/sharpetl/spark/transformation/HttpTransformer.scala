package com.github.sharpdata.sharpetl.spark.transformation

import com.fasterxml.jackson.databind.ObjectMapper
import com.github.sharpdata.sharpetl.spark.utils.{ETLSparkSession, HttpStatusUtils}
import com.jayway.jsonpath.JsonPath
import com.github.sharpdata.sharpetl.core.util.{ETLConfig, ETLLogger}
import net.minidev.json.JSONArray
import org.apache.http.HttpHost
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{HttpGet, HttpPost, HttpRequestBase}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client._
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

import java.net.URLEncoder

import scala.util.Using

object HttpTransformer extends Transformer {

  var httpClient: CloseableHttpClient = _

  lazy val mapper = new ObjectMapper

  override def transform(args: Map[String, String]): DataFrame = {
    val responseBody = getHttpResponseBody(args)
    transformerDF(responseBody, args)
  }

  private def getHttpResponseBody(args: Map[String, String]): String = {

    val httpProperties = HttpProperties.initHttpProperties(args)
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

  private def transformerDF(value: String, args: Map[String, String]): DataFrame = {
    val fieldName = args.getOrElse("fieldName", "value")
    val jsonPath = args.getOrElse("jsonPath", "$")
    val splitBy = args.getOrElse("splitBy", "")

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

  def initHttpProperties(args: Map[String, String]): HttpProperties = {
    val url = getEncodeUrl(args("url"))
    val httpMethod = args.getOrElse("httpMethod", "GET")
    val connectionName = args.get("connectionName")
    if (connectionName.nonEmpty) {
      val httpConnectionProperties = ETLConfig.getHttpProperties(connectionName.get)
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
      new HttpProperties(url, httpMethod, headers, Option(proxy), args)
    } else {
      new HttpProperties(url, httpMethod, Map.empty, Option.empty, args)
    }
  }
}

class HttpProperties(url: String, httpMethod: String, headers: Map[String, String], proxy: Option[HttpHost], optionalArgs: Map[String, String]) {

  def initRequest(): HttpRequestBase = {
    val httpRequest = httpMethod.toUpperCase match {
      case "GET" => new HttpGet(url)
      case _ =>
        val httpPost = new HttpPost(url)
        if (optionalArgs.contains("requestBody")) {
          val requestBody = optionalArgs.getOrElse("requestBody", "")
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
