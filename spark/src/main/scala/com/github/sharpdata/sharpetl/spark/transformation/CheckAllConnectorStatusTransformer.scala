package com.github.sharpdata.sharpetl.spark.transformation

import com.github.sharpdata.sharpetl.spark.utils.HttpStatusUtils
import com.github.sharpdata.sharpetl.core.exception.Exception.CheckFailedException
import com.github.sharpdata.sharpetl.core.util.{ETLConfig, ETLLogger}
import com.github.sharpdata.sharpetl.spark.utils.ETLSparkSession.sparkSession
import CheckConnectorStatusTransformer.checkConnectorRunning
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.spark.sql.DataFrame

import java.io.{BufferedReader, InputStreamReader}
import java.util.stream.Collectors


// $COVERAGE-OFF$
object CheckAllConnectorStatusTransformer extends Transformer {

  final case class CheckConnectorStatusException(message: String) extends RuntimeException(message)

  override def transform(args: Map[String, String]): DataFrame = {
    val truststoreLocation = ETLConfig.getProperty("truststore.location")
    System.setProperty("javax.net.ssl.trustStore", truststoreLocation)

    val uri: String = args("uri")
    val connectorNames = getConnectorList(uri)
    var disabledConnectors: Array[String] = Array.empty
    val httpclient: CloseableHttpClient = HttpClients.createDefault
    connectorNames.foreach(connectorName =>
      try {
        checkConnectorRunning(connectorName, uri, httpclient)
      }
      catch {
        case _: RuntimeException => disabledConnectors = disabledConnectors :+ connectorName
      })
    if (disabledConnectors.length > 0) {
      ETLLogger.error("Check Connector Status Failed, Some Connectors May Not Running")
      throw new CheckFailedException(s"Disabled Connector Names: ${disabledConnectors.mkString("{", ", ", "}")}")
    }
    else {
      ETLLogger.info("Check Connector finished, All Connector Running Normally")
    }

    sparkSession.emptyDataFrame
  }

  def getConnectorList(uri: String): Array[String] = {
    val endpoint = uri + "/connectors"

    val httpGet = new HttpGet(endpoint)
    val closeableHttpClient: CloseableHttpClient = HttpClients.createDefault()
    val closeableHttpResponse: CloseableHttpResponse = closeableHttpClient.execute(httpGet)
    try {
      if (!HttpStatusUtils.isSuccessful(closeableHttpResponse.getStatusLine)) {
        throw CheckConnectorStatusException(s"Get connector name list failed with statusCode ${closeableHttpResponse.getStatusLine.getStatusCode}")
      }
      val connectorNameString = new BufferedReader(
        new InputStreamReader(closeableHttpResponse.getEntity.getContent))
        .lines.collect(Collectors.joining(System.lineSeparator))
        .replace("\"", "")
        .replace("[", "")
        .replace("]", "")
      connectorNameString.split(",")
    } finally {
      closeableHttpResponse.close()
      closeableHttpClient.close()
    }
  }

}

// $COVERAGE-ON$
