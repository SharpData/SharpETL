package com.github.sharpdata.sharpetl.spark.datasource.jdbc

import com.github.sharpdata.sharpetl.core.api.Variables
import com.github.sharpdata.sharpetl.core.datasource.config.DBDataSourceConfig
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import org.scalatest.funspec.AnyFunSpec
import org.mockito.MockitoSugar
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.github.sharpdata.sharpetl.core.util.Constants.DataSourceType.POSTGRES
import com.github.sharpdata.sharpetl.core.util.Constants.WriteMode.APPEND
import org.mockito.stubbing.ReturnsDeepStubs

class AbstractJdbcDataSourceTest extends AnyFunSpec with MockitoSugar {

  val sourceConfig = new DBDataSourceConfig
  val sourceOptions = Map("queryTimeout" -> "5")
  sourceConfig.setDbName("psi")
  sourceConfig.setOptions(sourceOptions)

  val targetConfig = new DBDataSourceConfig
  targetConfig.setDbName("psi")
  targetConfig.setTableName("t_company")
  targetConfig.setOptions(sourceOptions)

  val sql = "select * from t_company limit 10"
  val step = new WorkflowStep
  step.setStep("1")
  step.setSourceConfig(sourceConfig)
  step.setTargetConfig(targetConfig)
  step.setSql(sql)
  step.setWriteMode(APPEND)

  describe("AbstractJdbcDataSource") {
    it("should call spark load with source options") {
      val dataSource = mock[AbstractJdbcDataSource]
      val spark = mock[SparkSession]

      val variables = Variables(collection.mutable.Map.empty[String, String])
      val expectOptions = Map("dbtable" -> s"($sql)").++(sourceOptions)

      when(dataSource.load(spark, step, variables)).thenCallRealMethod()
      when(dataSource.buildSelectSql(sql)).thenCallRealMethod()

      dataSource.load(spark, step, variables)
      verify(dataSource, times(1)).load(spark, expectOptions, "psi")
    }

    it("should call df save with target options") {
      val dataSource = spy(new MockPostgresDataSource)
      val df = mock[DataFrame](ReturnsDeepStubs)

      when(dataSource.getCols("psi", "t_company")).thenCallRealMethod()

      dataSource.save(df, step)

      val expectOptions = Map("dbtable" -> " ") ++ sourceOptions ++ Map("numPartitions" -> "8", "batchsize" -> "1024")
      verify(dataSource).buildJdbcConfig("psi", POSTGRES, expectOptions)
    }
  }
}
