package com.github.sharpdata.sharpetl.spark.end2end

import com.github.sharpdata.sharpetl.core.util.{DateUtil, ETLConfig, WorkflowReader}
import ETLSuit.runJob
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.mockito.ArgumentMatchers.anyString
import org.mockito.MockitoSugar.{when, withObjectMocked}
import org.scalatest.{BeforeAndAfterAll, DoNotDiscover}

import java.sql.Timestamp
import java.time.LocalDateTime

@DoNotDiscover
class PropertyLoadingSpec extends ETLSuit with BeforeAndAfterAll {

  val migrationMysql = new FixedMySQLContainer("mysql:5.7")
  val dataMysql = new FixedMySQLContainer("mysql:5.7")

  override protected def beforeEach(): Unit = {
    ETLConfig.reInitProperties()
  }

  override protected def beforeAll(): Unit = {
    ETLConfig.reInitProperties()
    migrationMysql.configurePort(migrationPort, "sharp_etl")
    migrationMysql.start()

    dataMysql.configurePort(dataPort, "int_test")
    dataMysql.start()
    execute(createTableSql)
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    execute("truncate table target")
    execute("truncate table source")
    migrationMysql.stop()
    dataMysql.stop()
    super.afterAll()
  }

  override val createTableSql: String =
    "CREATE TABLE IF NOT EXISTS target" +
      " (id int, value varchar(255), bz_time timestamp, job_id varchar(255), job_time varchar(255));"

  override val sourceDbName: String = "int_test"
  val sourceTableName: String = "source"
  override val targetDbName: String = "int_test"

  val startTime = LocalDateTime.now().minusDays(1L).format(DateUtil.L_YYYY_MM_DD_HH_MM_SS)

  val jobParameters: Array[String] = Array("single-job",
    "--name=source_to_target", "--period=1440",
    "--local", s"--default-start-time=${startTime}")


  val schema = List(
    StructField("id", IntegerType, true),
    StructField("value", StringType, true),
    StructField("bz_time", TimestampType, true)
  )

  val time = Timestamp.valueOf(LocalDateTime.of(2021, 10, 1, 0, 0, 0))

  val data = Seq(
    Row(1, "111", time),
    Row(2, "222", time)
  )

  val sampleDataDf = spark.createDataFrame(
    spark.sparkContext.parallelize(data),
    StructType(schema)
  )

  it("should replace variable through command line parameter and throw exception") {
    writeDataToSource(sampleDataDf, sourceTableName)
    val jobParametersWithExtra = jobParameters ++ Array("--once", "--env=test", "--override=mysql.password=XXXX,foo=bar,balabala=a=b=c=d")
    withObjectMocked[WorkflowReader.type]{
      when(WorkflowReader.readSteps(anyString())).thenReturn(Nil)
      runJob(jobParametersWithExtra)
    }
    assert(ETLConfig.getProperty("mysql.password") == "XXXX")
    assert(ETLConfig.getProperty("foo") == "bar")
    assert(ETLConfig.getProperty("balabala") == "a=b=c=d")
  }

  it("should read from local file system") {
    writeDataToSource(sampleDataDf, sourceTableName)
    val filePath = getClass.getResource("/application-test.properties").toString
    val jobParametersWithExtra = jobParameters ++ Array("--once", "--env=test", s"--property=$filePath")
    withObjectMocked[WorkflowReader.type]{
      when(WorkflowReader.readSteps(anyString())).thenReturn(Nil)
      runJob(jobParametersWithExtra)
    }
    assert(ETLConfig.getProperty("from_file_path") == "true")
    assert(ETLConfig.getProperty("flyway.password") == "admin")
  }
}
