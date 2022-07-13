package com.github.sharpdata.sharpetl.modeling.sql.gen

import com.github.sharpdata.sharpetl.modeling.excel.parser.DwdTableParser
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import AutoCreateDimSqlGen2.genAutoCreateDimStep
import DwdExtractSqlGen.genExtractStep
import DwdLoadSqlGen.genLoadStep
import DwdTransformSqlGen.genTargetSelectStep
import DwdTransformSqlGen2.{genMatchStep, generateReadMatchTableStep}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class WorkflowStepGenPostgresSpec extends AnyFlatSpec with should.Matchers {
  val uuidRegex = "[a-f\\d]{8,32}"


  it should "parse data modeling to SQL" in {
    val excelFilePath = this
      .getClass
      .getClassLoader
      .getResource("data-dict-v2-postgres.xlsx")
      .getPath

    val dwdModelings = DwdTableParser.readDwdConfig(excelFilePath)

    val factOrder = dwdModelings.head

    var steps = genExtractStep(factOrder, 1)

    toActualConfig(steps) should be(
      """-- step=1
        |-- source=postgres
        |--  dbName=ods
        |--  tableName=t_order
        |-- target=temp
        |--  tableName=t_order__extracted
        |-- writeMode=overwrite
        |select
        |	"order_id" as "order_id",
        |	"order_sn" as "order_sn",
        |	"product_code" as "product_code",
        |	"product_name" as "product_name",
        |	"product_version" as "product_version",
        |	"product_status" as "product_status",
        |	"user_code" as "user_code",
        |	"user_name" as "user_name",
        |	"user_age" as "user_age",
        |	"user_address" as "user_address",
        |	"product_count" as "product_count",
        |	"price" as "price",
        |	"discount" as "discount",
        |	"order_status" as "order_status",
        |	"order_create_time" as "order_create_time",
        |	"order_update_time" as "order_update_time",
        |	price - discount as "actual"
        |from "ods"."t_order"
        |where "job_id" = '${DATA_RANGE_START}';""".stripMargin)

    steps = genAutoCreateDimStep(steps, factOrder, 2)._1
    val autoCreateDimSteps = steps.tail
    toActualConfig(autoCreateDimSteps) should be(
      """-- step=2
        |-- source=transformation
        |--  className=com.github.sharpdata.sharpetl.spark.transformation.JdbcAutoCreateDimTransformer
        |--  methodName=transform
        |--   createDimMode=once
        |--   currentAndDimColumnsMapping={"order_create_time":"create_time","product_code":"mid","product_name":"name"}
        |--   currentAndDimPrimaryMapping={"product_code":"mid"}
        |--   currentBusinessCreateTime=order_create_time
        |--   dimDb=dim
        |--   dimDbType=postgres
        |--   dimTable=t_dim_product
        |--   dimTableColumnsAndType={"create_time":"timestamp","mid":"varchar(128)","name":"varchar(128)"}
        |--   updateTable=t_order__extracted
        |--  transformerType=object
        |-- target=do_nothing
        |
        |-- step=3
        |-- source=transformation
        |--  className=com.github.sharpdata.sharpetl.spark.transformation.JdbcAutoCreateDimTransformer
        |--  methodName=transform
        |--   createDimMode=always
        |--   currentAndDimColumnsMapping={"order_create_time":"create_time","user_code":"code","user_name":"name","user_age":"age","user_address":"address"}
        |--   currentAndDimPrimaryMapping={"user_code":"code"}
        |--   currentBusinessCreateTime=order_create_time
        |--   dimDb=dim
        |--   dimDbType=postgres
        |--   dimTable=t_dim_user
        |--   dimTableColumnsAndType={"create_time":"timestamp","code":"varchar(128)","name":"varchar(128)","age":"int","address":"varchar(128)"}
        |--   updateTable=t_order__extracted
        |--  transformerType=object
        |-- target=do_nothing""".stripMargin)

    steps = generateReadMatchTableStep(steps, factOrder, 4)._1
    val readMatchTableSteps = steps.slice(3,5)
    toActualConfig(readMatchTableSteps) should be(
      """-- step=4
        |-- source=postgres
        |--  dbName=dim
        |--  tableName=t_dim_product
        |-- target=temp
        |--  tableName=dim_t_dim_product__matched
        |-- writeMode=append
        |select
        | "id", "mid", "start_time", "end_time"
        |from "dim"."t_dim_product";
        |
        |-- step=5
        |-- source=postgres
        |--  dbName=dim
        |--  tableName=t_dim_user
        |-- target=temp
        |--  tableName=dim_t_dim_user__matched
        |-- writeMode=append
        |select
        | "dim_user_id", "user_info_code", "start_time", "end_time"
        |from "dim"."t_dim_user";""".stripMargin)

    steps = genMatchStep(steps, factOrder, 6)._1
    val matchStep = steps.last
    toActualConfig(List(matchStep)) should be(
      """-- step=6
        |-- source=temp
        |--  tableName=t_order__extracted
        |-- target=temp
        |--  tableName=t_order__joined
        |-- writeMode=append
        |select
        |	`t_order__extracted`.*,
        |	case when `dim_t_dim_product__matched`.`id` is null then '-1'
        |		else `dim_t_dim_product__matched`.`id` end as `product_id`,
        |	case when `dim_t_dim_user__matched`.`dim_user_id` is null then '-1'
        |		else `dim_t_dim_user__matched`.`dim_user_id` end as `user_id`
        |from `t_order__extracted`
        |left join `dim_t_dim_product__matched`
        | on `t_order__extracted`.`product_code` = `dim_t_dim_product__matched`.`mid`
        | and `t_order__extracted`.`order_create_time` >= `dim_t_dim_product__matched`.`start_time`
        | and (`t_order__extracted`.`order_create_time` < `dim_t_dim_product__matched`.`end_time`
        |      or `dim_t_dim_product__matched`.`end_time` is null)
        |
        |left join `dim_t_dim_user__matched`
        | on `t_order__extracted`.`user_code` = `dim_t_dim_user__matched`.`user_info_code`
        | and `t_order__extracted`.`order_create_time` >= `dim_t_dim_user__matched`.`start_time`
        | and (`t_order__extracted`.`order_create_time` < `dim_t_dim_user__matched`.`end_time`
        |      or `dim_t_dim_user__matched`.`end_time` is null);""".stripMargin)

    steps = genTargetSelectStep(steps, factOrder, 7)
    val targetStep = steps.last
    toActualConfig(List(targetStep)) should be(
      """-- step=7
        |-- source=temp
        |--  tableName=t_order__joined
        |-- target=temp
        |--  tableName=t_order__target_selected
        |-- writeMode=overwrite
        |select
        |	`order_id`,
        |	`order_sn`,
        |	`product_id`,
        |	`user_id`,
        |	`product_count`,
        |	`price`,
        |	`discount`,
        |	`order_status`,
        |	`order_create_time`,
        |	`order_update_time`,
        |	`actual`
        |from `t_order__joined`;""".stripMargin)

    steps = genLoadStep(steps, factOrder, 8)
    val loadStep = steps.last
    toActualConfig(List(loadStep)) should be(
      """-- step=8
        |-- source=transformation
        |--  className=com.github.sharpdata.sharpetl.spark.transformation.JdbcLoadTransformer
        |--  methodName=transform
        |--   businessCreateTime=order_create_time
        |--   businessUpdateTime=order_update_time
        |--   currentDb=dwd
        |--   currentDbType=postgres
        |--   currentTable=t_fact_order
        |--   currentTableColumnsAndType={"order_id":"varchar(128)","order_sn":"varchar(128)","product_id":"varchar(128)","user_id":"varchar(128)","product_count":"int","price":"decimal(10,4)","discount":"decimal(10,4)","order_status":"varchar(128)","order_create_time":"timestamp","order_update_time":"timestamp","actual":"decimal(10,4)"}
        |--   primaryFields=order_id,product_id
        |--   slowChanging=false
        |--   updateTable=t_order__target_selected
        |--   updateType=full
        |--  transformerType=object
        |-- target=do_nothing""".stripMargin)
  }


  def toActualConfig(steps: List[WorkflowStep]): String = {
    steps
      .map(_.toString.replaceAll(uuidRegex, "uuid"))
      .mkString("\n")
      .trim
  }
}
