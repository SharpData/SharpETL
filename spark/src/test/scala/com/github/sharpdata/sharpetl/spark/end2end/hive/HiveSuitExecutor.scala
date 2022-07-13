package com.github.sharpdata.sharpetl.spark.end2end.hive

import com.github.sharpdata.sharpetl.spark.end2end.FixedMySQLContainer
import com.github.sharpdata.sharpetl.core.repository.MyBatisSession
import com.github.sharpdata.sharpetl.core.util.ETLConfig
import org.scalatest.{BeforeAndAfterAll, DoNotDiscover, Sequential}

@DoNotDiscover
class HiveSuitExecutor extends Sequential(
  new DimStudentModelingSpec,
  new AutoCreateDimSpec,
  new FactEventModelingSpec
) with BeforeAndAfterAll {
  val migrationMysql = new FixedMySQLContainer("mysql:5.7")

  override protected def beforeAll(): Unit = {
    migrationMysql.configurePort(2333, "sharp_etl")
    migrationMysql.start()
    ETLConfig.reInitProperties()
    MyBatisSession.reloadFactory()
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    migrationMysql.stop()
    super.afterAll()
  }
}
