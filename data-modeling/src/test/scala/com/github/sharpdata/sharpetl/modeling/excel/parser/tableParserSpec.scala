package com.github.sharpdata.sharpetl.modeling.excel.parser

import com.github.sharpdata.sharpetl.modeling.formatConversion.createSqlParser.{createTableDDL, createTableDDLList}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class tableParserSpec extends AnyFlatSpec with should.Matchers {

  it should "create change" in {
    val filePath = this
      .getClass
      .getClassLoader
      .getResource("copyOfOds.xlsx")
      .getPath
    createTableDDLList(filePath).head._2 should be ("create table t_order(\norder_sn  varchar(128),\nproduct_code  varchar(128),\nproduct_name  varchar(128),\nproduct_version  varchar(128),\nproduct_status  varchar(128),\nuser_code  varchar(128),\nuser_name  varchar(128),\nuser_age  int,\nuser_address  varchar(128),\nproduct_count  int,\nprice  decimal(10, 4),\ndiscount  decimal(10, 4),\norder_status  varchar(128),\norder_create_time  timestamp,\norder_update_time  timestamp\n)")
    createTableDDLList(filePath)(1)._2 should be ("create table t_user(\nuser_code  varchar(128),\nuser_name  varchar(128),\nuser_age  int,\nuser_address  varchar(128),\ncreate_time  timestamp,\nupdate_time  timestamp\n)")
    createTableDDLList(filePath)(2)._2 should be ("create table t_product(\nproduct_code  varchar(128),\nproduct_name  varchar(128),\nproduct_version  varchar(128),\nproduct_status  varchar(128),\ncreate_time  timestamp,\nupdate_time  timestamp\n)")
  }
}
