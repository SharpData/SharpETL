package com.github.sharpdata.sharpetl.spark.end2end.delta

import com.github.sharpdata.sharpetl.core.repository.MyBatisSession
import com.github.sharpdata.sharpetl.core.util.ETLConfig
import com.github.sharpdata.sharpetl.spark.end2end.mysql.FixedMySQLContainer
import org.scalatest.{BeforeAndAfterAll, DoNotDiscover, Sequential}

@DoNotDiscover
class DeltaSuitExecutor extends Sequential(
  new DeltaLakeSpec,
  new FlyDeltaSpec
) with BeforeAndAfterAll {
  val logMysql = new FixedMySQLContainer("mysql:5.7")

  override protected def beforeAll(): Unit = {
    logMysql.configurePort(2333, "sharp_etl")
    logMysql.start()
    ETLConfig.reInitProperties()
    MyBatisSession.reloadFactory()
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    logMysql.stop()
    super.afterAll()
  }
}
