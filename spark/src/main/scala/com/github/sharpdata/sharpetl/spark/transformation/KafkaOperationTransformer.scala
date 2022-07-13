package com.github.sharpdata.sharpetl.spark.transformation

import com.github.sharpdata.sharpetl.spark.utils.HttpStatusUtils
import com.github.sharpdata.sharpetl.core.exception.Exception.CheckFailedException
import com.github.sharpdata.sharpetl.core.util.{ETLConfig, ETLLogger}
import com.github.sharpdata.sharpetl.spark.utils.ETLSparkSession.sparkSession
import CheckAllConnectorStatusTransformer.getConnectorList
import CheckConnectorStatusTransformer.{buildEndpoint, getStatusResponse}
import org.apache.http.client.methods.{HttpDelete, HttpPost, HttpPut}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.spark.sql.DataFrame


// $COVERAGE-OFF$
object KafkaOperationTransformer extends Transformer {


  override def transform(args: Map[String, String]): DataFrame = {
    val truststoreLocation = ETLConfig.getProperty("truststore.location")
    System.setProperty("javax.net.ssl.trustStore", truststoreLocation)

    val uri = ETLConfig.getKafkaProperties
    var connectorNames = args.getOrElse("connectorName", "").split(",")
    if (args.getOrElse("connectorName", "") == "") {
      connectorNames = getConnectorList(uri)
    }
    val httpclient: CloseableHttpClient = HttpClients.createDefault
    val operationCommand = args.getOrElse("operationCommand", "restart")
    var operationFailedConnectors: Array[String] = Array.empty
    connectorNames.foreach(connectorName =>
      try {
        runCommand(httpclient, uri, operationCommand, connectorName)
      }
      catch {
        case _: RuntimeException => operationFailedConnectors = operationFailedConnectors :+ connectorName
      }
    )
    if (operationFailedConnectors.length > 0) {
      ETLLogger.error(s"Some Connectors $operationCommand Failed")
      throw new CheckFailedException(s"$operationCommand Failed Connector Names: ${operationFailedConnectors.mkString("{", ", ", "}")}")
    }
    sparkSession.emptyDataFrame
  }

  private def runCommand(httpclient: CloseableHttpClient, uri: String, operationCommand: String,
                         connectorName: String): Unit = {
    operationCommand match {
      case "restart" => restartTask(httpclient, uri, connectorName)
      case "stop" => stopConnectors(httpclient, uri, connectorName)
      case "delete" => deleteConnectors(httpclient, uri, connectorName)
      case "resume" => resumeConnectors(httpclient, uri, connectorName)
      case _ => throw new CheckFailedException("Kafka Operation Command Not Found, Supported Command: restart, stop, delete")
    }
  }

  private def restartConnector(httpclient: CloseableHttpClient, uri: String, connectorName: String): Unit = {
    val endpoint = buildEndpoint(uri, "/connectors/" + connectorName + "/restart?includeTasks=true&onlyFailed=true")
    val httpPost = new HttpPost(endpoint)
    val response = httpclient.execute(httpPost)
    val statusCode = response.getStatusLine
    if (HttpStatusUtils.isSuccessful(statusCode)) {
      ETLLogger.info(s"Restart connector $connectorName succeed.")
      ETLLogger.info(s"The Response is ${response.toString}")
    } else {
      ETLLogger.error(s"Restart connector $connectorName failed.")
      ETLLogger.error(s"Error: ${response.getStatusLine.getReasonPhrase}")
      throw new CheckFailedException(s"Restart Connector $connectorName Failed, caused by " + response.getStatusLine.getReasonPhrase)
    }
  }

  private def restartTask(httpclient: CloseableHttpClient, uri: String, connectorName: String): Unit = {
    val kafkaResponse = getStatusResponse(httpclient, uri, connectorName)
    val taskId = kafkaResponse.getTaskId
    taskId.foreach(id => {
      val endpoint = buildEndpoint(uri, "/connectors/" + connectorName + "/tasks/" + id + "/restart")
      val response = httpclient.execute(new HttpPost(endpoint))
      if (HttpStatusUtils.isSuccessful(response.getStatusLine)) {
        ETLLogger.info(s"Restart connector $connectorName succeed, task id is $id")
      } else {
        ETLLogger.error(s"Restart connector $connectorName failed, task id is $id")
        throw new CheckFailedException(s"Restart Connector $connectorName Failed, caused by " + response.getStatusLine.getReasonPhrase)
      }
    })
  }

  private def stopConnectors(httpclient: CloseableHttpClient, uri: String, connectorName: String): Unit = {
    val endpoint = buildEndpoint(uri, "/connectors/" + connectorName + "/pause")
    val httpPut = new HttpPut(endpoint)
    val stopResponse = httpclient.execute(httpPut)
    val statusCode = stopResponse.getStatusLine
    if (HttpStatusUtils.isSuccessful(statusCode)) {
      ETLLogger.info(s"Stop connector $connectorName succeed.")
      ETLLogger.info(s"The Response is ${stopResponse.toString}")
    } else {
      ETLLogger.error(s"Stop connector $connectorName failed.")
      ETLLogger.error(s"Stop Error: ${stopResponse.getStatusLine.getReasonPhrase}")
      throw new CheckFailedException(s"Stop Connector $connectorName Failed, caused by " + stopResponse.getStatusLine.getReasonPhrase)
    }
  }

  private def deleteConnectors(httpclient: CloseableHttpClient, uri: String, connectorName: String): Unit = {
    val endpoint = buildEndpoint(uri, "/connectors/" + connectorName)
    val httpDelete = new HttpDelete(endpoint)
    val deleteResponse = httpclient.execute(httpDelete)
    val statusCode = deleteResponse.getStatusLine
    if (HttpStatusUtils.isSuccessful(statusCode)) {
      ETLLogger.info(s"Delete connector $connectorName succeed.")
      ETLLogger.info(s"The Delete Response is ${deleteResponse.toString}")
    } else {
      ETLLogger.error(s"Delete connector $connectorName failed.")
      ETLLogger.error(s"Delete Error: ${deleteResponse.getStatusLine.getReasonPhrase}")
      throw new CheckFailedException(s"Delete Connector $connectorName Failed, caused by " + deleteResponse.getStatusLine.getReasonPhrase)
    }
  }

  private def resumeConnectors(httpclient: CloseableHttpClient, uri: String, connectorName: String): Unit = {
    val endpoint = buildEndpoint(uri, "/connectors/" + connectorName + "/resume")
    val httpPut = new HttpPut(endpoint)
    val resumeResponse = httpclient.execute(httpPut)
    val statusCode = resumeResponse.getStatusLine
    if (HttpStatusUtils.isSuccessful(statusCode)) {
      ETLLogger.info(s"Resume connector $connectorName succeed.")
      ETLLogger.info(s"The Response is ${resumeResponse.toString}")
    } else {
      ETLLogger.error(s"Resume connector $connectorName failed.")
      ETLLogger.error(s"Resume Error: ${resumeResponse.getStatusLine.getReasonPhrase}")
      throw new CheckFailedException(s"Resume Connector $connectorName Failed, caused by " + resumeResponse.getStatusLine.getReasonPhrase)
    }
  }

}
// $COVERAGE-ON$
