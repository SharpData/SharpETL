package com.github.sharpdata.sharpetl.modeling.sql.gen

import com.github.sharpdata.sharpetl.modeling.excel.parser.DwdTableParser
import DwdWorkflowGen.genWorkflow

class WorkflowStepGenHiveSpec extends SqlUUIDSpec {

  it should "parse data modeling to SQL" in {
    val excelFilePath = this
      .getClass
      .getClassLoader
      .getResource("data-dict-v2-hive.xlsx")
      .getPath

    val dwdModelings = DwdTableParser.readDwdConfig(excelFilePath)

    val dimStudent = dwdModelings(1)

    genWorkflow(dimStudent, "dim_student")
      .toString.trim should be(readExpectConfig(s"tasks/dim_student.sql").trim)
  }
}
