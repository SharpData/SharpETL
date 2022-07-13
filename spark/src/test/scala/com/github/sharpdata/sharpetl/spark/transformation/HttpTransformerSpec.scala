package com.github.sharpdata.sharpetl.spark.transformation

import com.github.sharpdata.sharpetl.spark.end2end.ETLSuit
import com.github.sharpdata.sharpetl.core.util.ETLLogger
import ETLSuit.runJob
import org.mockserver.client.MockServerClient
import org.mockserver.model.HttpRequest
import org.mockserver.model.HttpRequest.request
import org.mockserver.model.HttpResponse.response
import org.scalatest.{BeforeAndAfterAll, DoNotDiscover}
import org.testcontainers.containers.MockServerContainer
import org.testcontainers.utility.DockerImageName

import scala.jdk.CollectionConverters._


@DoNotDiscover
class HttpTransformerSpec extends ETLSuit with BeforeAndAfterAll {

  private val mockHttpServer = new MockServerContainer(DockerImageName.parse("jamesdbloom/mockserver:mockserver-5.11.2"))

  override protected def beforeAll(): Unit = {
    mockHttpServer.setPortBindings(List("1080:1080").asJava)
    mockHttpServer.start()
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    mockHttpServer.stop()
    super.afterAll()
  }

  describe("LoopHttpTransformer") {
    val requestDef: HttpRequest = request().withPath("/report")
    it("should save in spark temp table") {
      new MockServerClient(mockHttpServer.getHost, mockHttpServer.getServerPort)
        .clear(requestDef)
        .when(requestDef)
        .respond(response()
          .withBody("{\"name\":\"zhangsan\",\"age\":18, \"address\":{\"province\":\"beijing\",\"city\":\"beijing\"}}"))

      val jobParameters: Array[String] = Array("single-job",
        "--name=loop_http_transformation_test", "--period=1440",
        "--local", s"--default-start-time=2021-11-28 15:30:30", "--env=test", "--once")

      runJob(jobParameters)
      val df_source_data = spark.sql("select * from `target_db`")

      assert(df_source_data.count() == 3)
    }

    it("should extract data by jsonPath") {
      new MockServerClient(mockHttpServer.getHost, mockHttpServer.getServerPort)
        .clear(requestDef)
        .when(requestDef)
        .respond(response()
          .withBody("{\"users\":[" +
            "{\"name\":\"zhangsan\",\"age\":18, \"address\":{\"province\":\"beijing\",\"city\":\"beijing\"}}, " +
            "{\"name\":\"zhangsan\",\"age\":18, \"address\":{\"province\":\"beijing\",\"city\":\"beijing\"}}" +
            "]}"))

      val jobParameters: Array[String] = Array("single-job",
        "--name=loop_http_transformation_json_path_test", "--period=1440",
        "--local", s"--default-start-time=2021-11-28 15:30:30", "--env=test", "--once")

      runJob(jobParameters)
      val df_source_data = spark.sql("select city from `target_db`")

      assert(df_source_data.count() == 6)
    }

    it("should extract data by jsonPath successful when data is empty") {
      new MockServerClient(mockHttpServer.getHost, mockHttpServer.getServerPort)
        .clear(requestDef)
        .when(requestDef)
        .respond(response()
          .withBody("{\"users\":[]}"))
      val jobParameters: Array[String] = Array("single-job",
        "--name=loop_http_transformation_json_path_test", "--period=1440",
        "--local", s"--default-start-time=2021-11-28 15:30:30", "--env=test", "--once")

      runJob(jobParameters)

    }
  }

  describe("HttpTransformer") {
    val requestDef: HttpRequest = request().withPath("/get_workday")
    val testJsonStr: String =
      """{
        |  "Report_Entry": [
        |    {
        |      "a": "a",
        |      "b": "b",
        |      "c": "c",
        |      "d": "d",
        |      "e": "e",
        |      "f": "f",
        |      "g": "g",
        |      "h": "h",
        |      "i": "i",
        |      "j": "j",
        |      "k": "k",
        |      "l": "l",
        |      "m": "m",
        |      "n": "n",
        |      "o": "o",
        |      "p": "p",
        |      "q": "q"
        |    },
        |    {
        |      "a": "a",
        |      "b": "b",
        |      "c": "c",
        |      "d": "d",
        |      "e": "e",
        |      "f": "f",
        |      "g": "g",
        |      "h": "h",
        |      "i": "i",
        |      "j": "j",
        |      "k": "k",
        |      "l": "l",
        |      "m": "m",
        |      "n": "n",
        |      "o": "o",
        |      "p": "p",
        |      "q": "q"
        |    }
        |  ]
        |}""".stripMargin
    val firstRow = "a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,2021,11,28,15"

    it("should save in spark temp table") {
      if (spark.version.startsWith("2.3")) {
        ETLLogger.error("`struct` complex type does NOT support Spark 2.3.x")
      } else {
        new MockServerClient(mockHttpServer.getHost, mockHttpServer.getServerPort)
          .clear(requestDef)
          .when(requestDef)
          .respond(response().withBody(testJsonStr))

        val jobParameters: Array[String] = Array("single-job",
          "--name=http_transformation_test", "--period=1440",
          "--local", s"--default-start-time=2021-11-28 15:30:30", "--env=test", "--once")

        runJob(jobParameters)
        val df_source_data = spark.sql("select * from `source_data`")
        val string_source_data = df_source_data.collect()(0)(0).toString
        val df_source_workday = spark.sql("select * from `source_data_workday`")
        val string_source_workday = df_source_workday.head().mkString(",")

        assert(string_source_data == testJsonStr)
        assert(string_source_workday == firstRow)
      }
    }

    it("should save variables in spark temp table") {
      new MockServerClient(mockHttpServer.getHost, mockHttpServer.getServerPort)
        .clear(requestDef)
        .when(requestDef)
        .respond(response().withBody(
          """
            |{
            |  "firstName": "John",
            |  "lastName" : "doe",
            |  "age"      : 26,
            |  "address"  : {
            |    "streetAddress": "naist street",
            |    "city"         : "Nara",
            |    "postalCode"   : "630-0192"
            |  },
            |  "phoneNumbers": [
            |    {
            |      "type"  : "iPhone",
            |      "number": "0123-4567-8888"
            |    },
            |    {
            |      "type"  : "home",
            |      "number": "0123-4567-8910"
            |    }
            |  ]
            |}
            |""".stripMargin))

      val jobParameters: Array[String] = Array("single-job",
        "--name=http_transformation_to_variables_test", "--period=1440",
        "--local", s"--default-start-time=2021-11-28 15:30:30", "--env=test", "--once")

      runJob(jobParameters)
      val df_types = spark.sql("select `types` from `target_data_types`")
      val string_types = df_types.head().mkString(",")

      assert(string_types == "iPhone__home")
    }
  }

  override val createTableSql: String = ""
}
