import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.github.sharpdata.sharpetl.spark.transformation.{HttpBadRequestException, HttpProperties, Transformer}
import com.github.sharpdata.sharpetl.spark.utils.{ETLSparkSession, HttpStatusUtils}
import com.github.sharpdata.sharpetl.spark.transformation._
import com.jayway.jsonpath.JsonPath
import com.github.sharpdata.sharpetl.core.util.ETLLogger
import net.minidev.json.JSONArray
import org.apache.http.impl.client._
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.DataFrame

object LoopHttpTransformer extends Transformer {

  var httpClient: CloseableHttpClient = _

  val mapper = new ObjectMapper with ScalaObjectMapper
  mapper.setSerializationInclusion(Include.NON_ABSENT)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  mapper.registerModule(DefaultScalaModule)

  override def transform(args: Map[String, String]): DataFrame = {
    require(args.contains("items"))
    require(args.contains("splitBy"))
    require(args.contains("varName"))

    val splitBy = args("splitBy")
    val items = args("items").split(splitBy)
    val varName = args("varName")
    items.map(item => {
      val updatedArgs = args
        .filter { case (_, value) => value != null }
        .map { case (key, value) =>
          val replacedValue = value.replaceAllLiterally("${" + varName + "}", item)
          key -> replacedValue
        }
      val responseBody = getHttpResponseBody(updatedArgs)
      transformerDF(responseBody, args)
    })
      .reduceOption((df1, df2) => df1.union(df2))
      .getOrElse(ETLSparkSession.sparkSession.emptyDataFrame)

  }

  private def getHttpResponseBody(args: Map[String, String]): String = {
    val httpProperties = HttpProperties.initHttpProperties(args)
    val httpRequest = httpProperties.initRequest()
    ETLLogger.info(s"url: ${httpRequest.getURI.toString}")
    if (httpClient == null) {
      httpClient = HttpClients.createDefault()
    }
    val response = httpClient.execute(httpRequest)
    if (!HttpStatusUtils.isSuccessful(response.getStatusLine)) {
      throw new HttpBadRequestException(s"${httpRequest.getURI.toString} failed with statusCode ${response.getStatusLine.getStatusCode}")
    }

    EntityUtils.toString(response.getEntity)
  }

  private def transformerDF(value: String, args: Map[String, String]): DataFrame = {
    val jsonPath = args.getOrElse("jsonPath", "$")

    val values = JsonPath.read[Object](value, jsonPath)
    val spark = ETLSparkSession.sparkSession
    import spark.implicits._
    values match {
      case jsonArray: JSONArray =>
        jsonArray.toArray()
          .map(x => {
            val jsonDataset = spark.createDataset(mapper.writeValueAsString(x) :: Nil)
            spark.read.json(jsonDataset)
          })
          .reduceOption((df1, df2) => df1.union(df2))
          .getOrElse(spark.emptyDataFrame)
      case _ =>
        val jsonDataset = spark.createDataset(mapper.writeValueAsString(values) :: Nil)
        spark.read.json(jsonDataset)
    }
  }

}