package com.github.sharpdata.sharpetl.spark.datasource

import com.github.sharpdata.sharpetl.core.util.ETLLogger
import com.github.sharpdata.sharpetl.spark.end2end.ETLSuit
import com.github.sharpdata.sharpetl.spark.end2end.ETLSuit.runJob
import org.mockserver.client.MockServerClient
import org.mockserver.model.HttpRequest
import org.mockserver.model.HttpRequest.request
import org.mockserver.model.HttpResponse.response
import org.scalatest.{BeforeAndAfterAll, DoNotDiscover}
import org.testcontainers.containers.MockServerContainer
import org.testcontainers.utility.DockerImageName

import scala.jdk.CollectionConverters._


@DoNotDiscover
class HttpDataSourceSpec extends ETLSuit with BeforeAndAfterAll {

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

  describe("HttpDataSource") {
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
          "--name=http_datasource", "--period=1440",
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
        "--name=http_datasource_to_variables", "--period=1440",
        "--local", s"--default-start-time=2021-11-28 15:30:30", "--env=test", "--once")

      runJob(jobParameters)
      val df_types = spark.sql("select `types` from `target_data_types`")
      val string_types = df_types.head().mkString(",")

      assert(string_types == "iPhone__home")
    }
  }

  describe("loop over HttpDataSource") {
    def requestDef(tableName: String): HttpRequest = request().withPath(s"/get_from_table/$tableName")

    def testJsonStr(tableName: String): String =
      s"""{
         |  "result": "result_of_$tableName"
         |}""".stripMargin

    it("should send loop request") {
      if (spark.version.startsWith("2.3")) {
        ETLLogger.error("`struct` complex type does NOT support Spark 2.3.x")
      } else {
        val mockServerClient = new MockServerClient(mockHttpServer.getHost, mockHttpServer.getServerPort)
        (1 to 4).foreach(it => {

          val req = requestDef(s"test_$it")
          mockServerClient
            .clear(req)
            .when(req)
            .respond(response().withBody(testJsonStr(s"test_$it")))

        })

        val jobParameters: Array[String] = Array("single-job",
          "--name=http_loop_request", "--period=1440",
          "--local", s"--default-start-time=2021-11-28 15:30:30", "--env=test", "--once")

        runJob(jobParameters)
        spark
          .sql("select * from `target_temp_table`")
          .select("result")
          .toLocalIterator().asScala.toList.mkString("\n") should be(
          """[{
            |  "result": "result_of_test_1"
            |}]
            |[{
            |  "result": "result_of_test_2"
            |}]
            |[{
            |  "result": "result_of_test_3"
            |}]
            |[{
            |  "result": "result_of_test_4"
            |}]""".stripMargin
        )
      }
    }
  }

  override val createTableSql: String = ""
}
