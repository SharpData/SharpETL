package com.github.sharpdata.sharpetl.modeling.sql.gen

import com.github.sharpdata.sharpetl.modeling.excel.parser.OdsTableParser

class OdsWorkflowGenSpecNoRowFilter extends SqlUUIDSpec {
  it should "parse source to ods without row Filter Expression  => SQL" in {
    val excelFilePath = this
      .getClass
      .getClassLoader
      .getResource("ods-template.xlsx")
      .getPath
    val odsModelings = OdsTableParser.readOdsConfig(excelFilePath)
    val use = odsModelings(1)
    val workflow = OdsWorkflowGen.genWorkflow(use, "t_use")
    workflow.toString.trim should be(readExpectConfig(s"tasks/t_use.sql").trim)
  }
}

