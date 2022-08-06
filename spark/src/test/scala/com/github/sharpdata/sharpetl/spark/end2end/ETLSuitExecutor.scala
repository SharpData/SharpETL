package com.github.sharpdata.sharpetl.spark.end2end

import com.github.sharpdata.sharpetl.spark.transformation._
import com.github.sharpdata.sharpetl.core.repository.MyBatisSession
import com.github.sharpdata.sharpetl.core.util.ETLConfig
import com.github.sharpdata.sharpetl.spark.datasource.HttpDataSourceSpec
import org.scalatest.{BeforeAndAfterAll, DoNotDiscover, Suites}
import org.testcontainers.containers.MySQLContainer

// All suite in Suites are run in parallel, need to use Sequential if we want them to run in order
@DoNotDiscover
class ETLSuitExecutor extends Suites(
  new DailyJobsSummaryReportTransformSpec,
  new HttpDataSourceSpec,
  new DynamicLoadingTransformerSpec,
  new Source2TargetSpec,
  new BatchJobSpec,
  new IncrementalDiffModeSpec,
  new IncrementalDiffModeSplitSpec,
  new DataQualityCheckSpec,
  new TaskDependenciesSpec,
  new SkipRunningJobSpec,
  new IncrementalAutoIncIDModeSpec,
  new JdbcTransformerSpec,
  new SparkSessionIsolationSpec,
  new ReplaceTemplateTableNameSpec,
  new DeltaLakeSpec,
  new UDFSpec
) with BeforeAndAfterAll {

  val migrationMysql = new FixedMySQLContainer("mysql:5.7")
  val dataMysql = new FixedMySQLContainer("mysql:5.7")

  override protected def beforeAll(): Unit = {
    migrationMysql.configurePort(2333, "sharp_etl")
    migrationMysql.start()

    dataMysql.configurePort(2334, "int_test")
    dataMysql.start()

    ETLConfig.reInitProperties()
    MyBatisSession.reloadFactory()

    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    migrationMysql.stop()
    dataMysql.stop()
    super.afterAll()
  }
}

class FixedMySQLContainer(val dockerImageName: String) extends MySQLContainer(dockerImageName) {
  def configurePort(port: Int, dbName: String): FixedMySQLContainer = {
    super.addFixedExposedPort(port, 3306)
    super.withEnv("MYSQL_ROOT_PASSWORD", "root")
    //super.withEnv("TZ", "Asia/Shanghai")
    super.withUsername("admin")
    super.withPassword("admin")
    super.withDatabaseName(dbName)
    super.withReuse(true)
    this
  }
}
