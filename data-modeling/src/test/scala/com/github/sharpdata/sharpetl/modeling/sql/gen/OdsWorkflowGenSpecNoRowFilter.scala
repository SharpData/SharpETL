package com.github.sharpdata.sharpetl.modeling.sql.gen

import com.github.sharpdata.sharpetl.modeling.excel.parser.OdsTableParser

class OdsWorkflowGenSpecNoRowFilter extends SqlUUIDSpec {
  it should "parse source to ods without row Filter Expression  => SQL" in {
    val excelFilePath = this
      .getClass
      .getClassLoader
      .getResource("sharp-etl-Quick-Start-Guide.xlsx")
      .getPath

    val odsModelings = OdsTableParser.readOdsConfig(excelFilePath)

    val example = odsModelings.head

    val workflow = OdsWorkflowGen.genWorkflow(example, "sharp-etl-Quick-Start-Guide")
    workflow.toString.trim should be(readExpectConfig(s"tasks/sharp-etl-Quick-Start-Guide.sql").trim)
  }
}

