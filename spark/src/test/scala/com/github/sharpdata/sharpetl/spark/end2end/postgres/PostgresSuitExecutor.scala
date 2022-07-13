package com.github.sharpdata.sharpetl.spark.end2end.postgres

import com.github.sharpdata.sharpetl.spark.end2end.FixedMySQLContainer
import com.github.sharpdata.sharpetl.core.repository.MyBatisSession
import com.github.sharpdata.sharpetl.core.util.ETLConfig
import org.scalatest.{BeforeAndAfterAll, DoNotDiscover, Suites}

@DoNotDiscover
class PostgresSuitExecutor extends Suites(
  new PostgresModelingSpec
) with BeforeAndAfterAll {
  val postgresContainer = new FixedPostgresContainer("postgres:12.0-alpine")
  val mysqlContainer = new FixedMySQLContainer("mysql:5.7")

  override protected def beforeAll(): Unit = {
    mysqlContainer.configurePort(2333, "sharp_etl")
    mysqlContainer.start()

    postgresContainer.configurePort(5432, "postgres")
    postgresContainer.start()

    ETLConfig.reInitProperties()
    MyBatisSession.reloadFactory()
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    mysqlContainer.stop()
    postgresContainer.stop()
    super.afterAll()
  }
}
