package com.github.sharpdata.sharpetl.spark.transformation

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.github.sharpdata.sharpetl.spark.utils.HttpStatusUtils
import com.github.sharpdata.sharpetl.core.exception.Exception.CheckFailedException
import com.github.sharpdata.sharpetl.core.util.{ETLConfig, ETLLogger}
import com.github.sharpdata.sharpetl.spark.utils.ETLSparkSession.sparkSession
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.DataFrame

import java.nio.charset.StandardCharsets

// $COVERAGE-OFF$
object CheckConnectorStatusTransformer extends Transformer {

  override def transform(args: Map[String, String]): DataFrame = {
    val truststoreLocation = ETLConfig.getProperty("truststore.location")
    System.setProperty("javax.net.ssl.trustStore", truststoreLocation)

    val connectorName = args.getOrElse("connectorName", "").split(",")
    val uri = ETLConfig.getKafkaProperties
    val httpclient: CloseableHttpClient = HttpClients.createDefault
    try {
      connectorName.foreach(connector => checkConnectorRunning(connector.trim, uri, httpclient))
    }
    finally {
      if (httpclient != null) httpclient.close()
    }
    sparkSession.emptyDataFrame
  }

  def checkConnectorRunning(connectorName: String, uri: String, httpclient: CloseableHttpClient): Unit = {
    val statusResponse = getStatusResponse(httpclient, uri, connectorName)
    if (statusResponse.isRunning) {
      ETLLogger.info(s"$connectorName is running normally.")
    } else {
      ETLLogger.error(statusResponse.toString)
      throw new CheckFailedException(s"Check Status Failed $connectorName may not running")
    }
  }

  def getStatusResponse(httpclient: CloseableHttpClient, uri: String, connectorName: String): KafkaStatusResponse = {
    val endpoint = buildEndpoint(uri, "/connectors/" + connectorName + "/status")
    val httpGet = new HttpGet(endpoint)
    val response = httpclient.execute(httpGet)
    val statusCode = response.getStatusLine
    if (HttpStatusUtils.isSuccessful(statusCode)) {
      val responseBody = EntityUtils.toString(response.getEntity, StandardCharsets.UTF_8)
      val objectMapper = new ObjectMapper()
      objectMapper.registerModule(DefaultScalaModule)
      objectMapper.readValue(responseBody, classOf[KafkaStatusResponse])
    } else {
      ETLLogger.error(s"Get connector $connectorName status failed.")
      ETLLogger.error(s"Causes: ${response.getStatusLine.getReasonPhrase}")
      throw new CheckFailedException(s"Check $connectorName Status Failed, caused by " + response.getStatusLine.getReasonPhrase)
    }
  }

  def buildEndpoint(uri: String, path: String): String = {
    if (uri.endsWith("/") && path.startsWith("/")) {
      uri.substring(0, uri.length - 1) + path
    } else {
      uri + path
    }
  }
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class KafkaStatusResponse(name: String,
                               connector: Connector,
                               tasks: List[Task]) {

  override def toString: String = {
    "{" +
      "name='" + name + '\'' +
      ", connector=" + connector.toString() +
      ", tasks=" + tasks.toString() +
      '}'
  }

  def isRunning: Boolean = {
    if (tasks.isEmpty) {
      false
    }
    else {
      val taskRunningList = tasks.filter(task => task.state.equals(ConnectorState.RUNNING.toString))
      connector.state.equals(ConnectorState.RUNNING.toString) && taskRunningList.size == tasks.size
    }
  }

  def getTaskId: List[Int] = {
    tasks.map(task => task.id)
  }
}

@JsonIgnoreProperties(ignoreUnknown = true)
class Connector {
  var state: String = _
  override def toString: String = {
    "{" +
      "state='" + state + '\'' +
      '}'
  }
}

@JsonIgnoreProperties(ignoreUnknown = true)
class Task {

  var id: Int = _
  var state: String = _
  var trace: String = _

  override def toString: String = {
    "{" +
      "id=" + id +
      ", state='" + state + '\'' +
      ", trace='" + trace + '\'' +
      '}'
  }
}

object ConnectorState extends Enumeration {
  type ConnectorState = Value
  val UNASSIGNED, RUNNING, PAUSED, FAILED = Value
}

// $COVERAGE-ON$
