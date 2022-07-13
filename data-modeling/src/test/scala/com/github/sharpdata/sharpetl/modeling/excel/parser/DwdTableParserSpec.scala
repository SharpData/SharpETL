package com.github.sharpdata.sharpetl.modeling.excel.parser

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class DwdTableParserSpec extends AnyFlatSpec with should.Matchers {

  it should "read excel and encapsulate as objects" in {
    val filePath = this
      .getClass
      .getClassLoader
      .getResource("data-dict-v2-hive.xlsx")
      .getPath
    val modelings = DwdTableParser.readDwdConfig(filePath)
    modelings.size should be(3)

    val modeling = modelings.head

    val dwdTableConfig = modeling.dwdTableConfig
    dwdTableConfig.sourceTable should be("t_order")
    dwdTableConfig.targetTable should be("t_fact_order")
    dwdTableConfig.factOrDim should be("fact")

    val columns = modeling.columns

    columns.size should be(26) // scalastyle:ignore

    val rowNumber3 = columns(2)
    rowNumber3.extraColumnExpression should be("zip_id_flag")
    rowNumber3.joinDb should be("dim")
    rowNumber3.joinTable should be("t_dim_product")
    rowNumber3.createDimMode should be("always")
  }
}
