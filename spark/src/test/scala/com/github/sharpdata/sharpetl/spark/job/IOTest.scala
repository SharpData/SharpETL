package com.github.sharpdata.sharpetl.spark.job

import com.github.sharpdata.sharpetl.core.api.Variables
import com.github.sharpdata.sharpetl.core.datasource.config.DataSourceConfig
import com.github.sharpdata.sharpetl.core.repository.model.JobLog
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import org.junit.Assert.assertEquals
import org.mockito.Mockito.mock
import org.scalatest.funspec.AnyFunSpec


class IOTest extends AnyFunSpec with SparkSessionTestWrapper {

  it("should add Derived Columns after reading from source") {
    val processJobStep = new WorkflowStep()
    val dataSourceConfig = new DataSourceConfig()
    dataSourceConfig.derivedColumns = "a:10;b:20"
    dataSourceConfig.dataSourceType = "temp"
    processJobStep.sql =
      """
         select "2021-10-21" as `day`
        """.stripMargin

    processJobStep.source = dataSourceConfig
    val df = IO.read(spark, processJobStep, Variables.empty, mock(classOf[JobLog]))

    assertEquals(3, df.schema.length)
    assertEquals("day", df.schema(0).name)
    assertEquals("a", df.schema(1).name)
    assertEquals("b", df.schema(2).name)

    val row = df.collect()(0)
    assertEquals("2021-10-21", row(0))
    assertEquals("10", row(1))
    assertEquals("20", row(2))
  }
}