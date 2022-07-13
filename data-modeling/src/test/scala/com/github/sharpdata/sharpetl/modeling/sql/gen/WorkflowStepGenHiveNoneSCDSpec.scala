package com.github.sharpdata.sharpetl.modeling.sql.gen

import com.github.sharpdata.sharpetl.modeling.excel.parser.DwdTableParser
import DwdWorkflowGen.genWorkflow

class WorkflowStepGenHiveNoneSCDSpec extends SqlUUIDSpec {


  it should "parse data modeling to SQL" in {
    val excelFilePath = this
      .getClass
      .getClassLoader
      .getResource("data-dict-v2-event-hive.xlsx")
      .getPath

    val dwdModelings = DwdTableParser.readDwdConfig(excelFilePath)

    val eventModeling = dwdModelings.head

    genWorkflow(eventModeling, "fact_event")
      .toString.trim should be(readExpectConfig(s"tasks/fact_event.sql").trim)

  }
}
