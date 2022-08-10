package com.github.sharpdata.sharpetl.modeling.sql.gen

import com.github.sharpdata.sharpetl.modeling.excel.parser.OdsTableParser

class OdsWorkflowGenSpecMoreTypeRowFilter extends SqlUUIDSpec {
  it should "parse source to ods with two type row Filter Expression  => SQL" in {
    val excelFilePath = this
      .getClass
      .getClassLoader
      .getResource("ods-template3.xlsx")
      .getPath
    val odsModelings = OdsTableParser.readOdsConfig(excelFilePath)
    val example = odsModelings.head
    val workflow = OdsWorkflowGen.genWorkflow(example, "ods-template3")

    workflow.toString.trim should be(readExpectConfig(s"tasks/ods-template3.sql").trim)
  }
}
