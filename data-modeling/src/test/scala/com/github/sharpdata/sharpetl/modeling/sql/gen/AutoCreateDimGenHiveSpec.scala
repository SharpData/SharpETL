package com.github.sharpdata.sharpetl.modeling.sql.gen

import com.github.sharpdata.sharpetl.modeling.excel.parser.DwdTableParser
import DwdWorkflowGen.genWorkflow

class AutoCreateDimGenHiveSpec extends SqlUUIDSpec {

  it should "parse data modeling to SQL" in {
    val excelFilePath = this
      .getClass
      .getClassLoader
      .getResource("data-dict-v2-hive.xlsx")
      .getPath

    val dwdModelings = DwdTableParser.readDwdConfig(excelFilePath)

    val order = dwdModelings.head

    genWorkflow(order, "auto_create_dim").toString.trim should be(readExpectConfig(s"tasks/auto_create_dim.sql").trim)

  }
}
