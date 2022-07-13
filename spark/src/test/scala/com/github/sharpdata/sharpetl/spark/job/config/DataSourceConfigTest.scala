package com.github.sharpdata.sharpetl.spark.job.config

import com.github.sharpdata.sharpetl.core.datasource.config.DBDataSourceConfig
import org.scalatest.funspec.AnyFunSpec

class DataSourceConfigTest extends AnyFunSpec {

  describe("DataSourceConfig") {
    it("should have an options property") {
      val config = new DBDataSourceConfig
      assert(config.options.isEmpty)
    }
  }
}
