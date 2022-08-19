package com.github.sharpdata.sharpetl.modeling.excel.parser

import com.github.sharpdata.sharpetl.modeling.formatConversion.createSqlParser.{createTable, createTableList, typeConversion, typeConversionFilePath}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class tableParserSpec extends AnyFlatSpec with should.Matchers {
  it should "sql change" in {
val filePath = this
  .getClass
  .getClassLoader
  .getResource("copyOfOds.xlsx")
  .getPath
  typeConversion(filePath).flatMap(it=>it._2.map(item=>item.targetType)) should be (List("varchar(128)","varchar(128)", "varchar(128)","varchar(128)","varchar(128)","varchar(128)","varchar(128)","int","varchar(128)","int","decimal(10, 4)", "decimal(10, 4)","varchar(128)","timestamp","timestamp","varchar(128)", "varchar(128)","int","varchar(128)","timestamp","timestamp","varchar(128)","varchar(128)", "varchar(128)","varchar(128)","timestamp","timestamp"))
  }

  it should "create change" in {
    val filePath = this
      .getClass
      .getClassLoader
      .getResource("copyOfOds.xlsx")
      .getPath
    createTableList(filePath).head._2 should be ("create table t_order\n(\n       order_sn  varchar(128)  primary key,\n       product_code  varchar(128),\n       product_name  varchar(128),\n       product_version  varchar(128),\n       product_status  varchar(128),\n       user_code  varchar(128),\n       user_name  varchar(128),\n       user_age  int,\n       user_address  varchar(128),\n       product_count  int,\n       price  decimal(10, 4),\n       discount  decimal(10, 4),\n       order_status  varchar(128),\n       order_create_time  timestamp,\n       order_update_time  timestamp  auto_increment\n);")
    createTableList(filePath)(1)._2 should be ("create table t_user\n(\n       user_code  varchar(128),\n       user_name  varchar(128),\n       user_age  int,\n       user_address  varchar(128),\n       create_time  timestamp,\n       update_time  timestamp  auto_increment\n);")
    createTableList(filePath)(2)._2 should be ("create table t_product\n(\n       product_code  varchar(128),\n       product_name  varchar(128),\n       product_version  varchar(128),\n       product_status  varchar(128),\n       create_time  timestamp,\n       update_time  timestamp  auto_increment\n);")
  }
}
