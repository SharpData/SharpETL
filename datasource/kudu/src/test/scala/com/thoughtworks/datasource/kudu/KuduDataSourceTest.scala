package com.github.sharpdata.sharpetl.datasource.kudu

import com.github.sharpdata.sharpetl.datasource.kudu.KuduDataSource
import com.github.sharpdata.sharpetl.core.datasource.config.DBDataSourceConfig
import org.scalatest.PrivateMethodTester
import org.scalatest.funspec.AnyFunSpec

class KuduDataSourceTest extends AnyFunSpec with PrivateMethodTester {
  describe("buildKuduLoadOptions") {
    for ((sourceType, value) <- Map("kudu" -> "temp", "impala_kudu" -> "impala::smc.temp")) {
      it(s"should build options correctly for source type ${sourceType}") {
        val sourceConfig = new DBDataSourceConfig()
        sourceConfig.setDataSourceType(sourceType)
        sourceConfig.setDbName("smc")
        sourceConfig.setTableName("temp")
        val sourceOptions = Map("testArg" -> "build")
        sourceConfig.setOptions(sourceOptions)
        val options =
          new KuduDataSource(null).buildKuduLoadOptions(sourceConfig)

        assert(options.size == 3)
        assert(options("testArg") == "build")
        assert(options("kudu.table") == value)
      }
    }
  }
}
