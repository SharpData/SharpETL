package com.github.sharpdata.sharpetl.modeling.sql.gen

import com.github.sharpdata.sharpetl.modeling.excel.parser.DwdTableParser
import DwdWorkflowGen.genWorkflow

class WorkflowStepGenHiveFactSpec extends SqlUUIDSpec {


  it should "parse data modeling to SQL" in {
    val excelFilePath = this
      .getClass
      .getClassLoader
      .getResource("data-dict-v2-hive.xlsx")
      .getPath

    val dwdModelings = DwdTableParser.readDwdConfig(excelFilePath)

    val factDevice = dwdModelings(2)

    genWorkflow(factDevice, "fact_device")
      .toString.trim should be(readExpectConfig(s"tasks/fact_device.sql").trim)

  }
}
