package com.github.sharpdata.sharpetl.modeling.sql.gen

import com.github.sharpdata.sharpetl.modeling.excel.parser.OdsTableParser

class OdsWorkflowGenSpec extends SqlUUIDSpec {
  it should "parse source to ods => SQL" in {
    val excelFilePath = this
      .getClass
      .getClassLoader
      .getResource("ods-template.xlsx")
      .getPath

    val odsModelings = OdsTableParser.readOdsConfig(excelFilePath)

    val example = odsModelings.head

    val workflow = OdsWorkflowGen.genWorkflow(example, "ods-template")

    workflow.toString.trim should be(readExpectConfig(s"tasks/ods-template.sql").trim)
  }
}
