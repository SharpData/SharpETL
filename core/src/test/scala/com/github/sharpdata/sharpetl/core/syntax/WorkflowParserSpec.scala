package com.github.sharpdata.sharpetl.core.syntax

import com.github.sharpdata.sharpetl.core.datasource.config._
import fastparse._
import com.github.sharpdata.sharpetl.core.syntax.WorkflowParser._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should

class WorkflowParserSpec extends AnyFunSpec with should.Matchers {
  it("parse invalid workflow without step to error") {
    val text =
      """
        |-- workflow=abc
        |--  period=1440
        |--  loadType=incremental/full
        |--  logDrivenType=upstream/timewindow/diff/kafka/inc...
        |--  upstream=ods_product
        |--  dependsOn=dim_user,dim_price
        |--  comment=this ETL script is working for xxx
        |--  timeout=3600
        |--  defaultStart=20220101
        |--  stopScheduleWhenFail=false
        |--  options
        |--   project=abc
        |--  notify
        |--   notifyType=email
        |--   recipients=a@q.com,b@q.com
        |--   notifyCondition=SUCCESS/FAILURE/ALWAYS
        |""".stripMargin

    val r = parseWorkflow(text)
    r.asInstanceOf[WFParseFail].toString should be(
      """18:1: error: Expected parse by `stepHeader` at 18:1, but found "".
        |Parse stack is workflow:1:1 / step:18:1 / stepHeader:18:1
        |18|--   notifyCondition=SUCCESS/FAILURE/ALWAYS
        |  |^""".stripMargin)
  }

  it("parse invalid workflow to error") {
    val text =
      """
        |-- workflow=abc
        |--  period=1440
        |--  loadType
        |--  logDrivenType=upstream/timewindow/diff/kafka/inc...
        |--  upstream=ods_product
        |--  dependsOn=dim_user,dim_price
        |--  comment=this ETL script is working for xxx
        |--  timeout=3600
        |--  defaultStart=20220101
        |--  stopScheduleWhenFail=false
        |""".stripMargin

    val r = parseWorkflow(text)
    //println(r.asInstanceOf[WFParseFail].toString)
    r.asInstanceOf[WFParseFail].toString should be(
      """4:1: error: Expected parse by `stepHeader` at 4:1, but found "--  loadTy".
        |Parse stack is workflow:1:1 / step:4:1 / stepHeader:4:1
        |4|--  loadType
        | |^""".stripMargin)
  }

  it("parse sample workflow") {
    val text =
      """
        |-- workflow=same_with_file_name
        |--  period=1440
        |--  loadType=incremental/full
        |--  logDrivenType=upstream/timewindow/diff/kafka/inc...
        |--  upstream=ods_product
        |--  dependsOn=dim_user,dim_price
        |--  comment=this ETL script is working for xxx
        |--  timeout=3600
        |--  defaultStart=20220101
        |--  stopScheduleWhenFail=false
        |--  options
        |--   project=abc
        |--  notify
        |--   notifyType=email
        |--   recipients=a@q.com,b@q.com
        |--   notifyCondition=SUCCESS/FAILURE/ALWAYS
        |
        |-- step=read csv
        |-- source=csv
        |-- target=csv
        |
        |""".stripMargin

    val header = parse(text, workflow(_))
    header.isSuccess should be(true)
    val wf = header.get.value
    wf.name should be("same_with_file_name")
    wf.options.size should be(1)
    wf.notifies.head.recipients should be("a@q.com,b@q.com")
    wf.timeout should be(3600)
  }

  it("parse simple workflow") {
    val text =
      """
        |-- workflow=same_with_file_name
        |--  period=1440
        |--  loadType=incremental/full
        |--  logDrivenType=upstream/timewindow/diff/kafka/inc...
        |--  upstream=ods_product
        |--  dependsOn=dim_user,dim_price
        |--  defaultStart=20220101
        |--  notify
        |--   notifyType=email
        |--   recipients=a@q.com,b@q.com
        |--   notifyCondition=SUCCESS/FAILURE/ALWAYS
        |--  notify
        |--   notifyType=email
        |--   recipients=c@q.com,d@q.com
        |--   notifyCondition=ALWAYS
        |
        |-- step=read csv
        |-- source=csv
        |-- target=csv
        |
        |""".stripMargin

    val header = parseWorkflow(text)
    //println(header.asInstanceOf[WFParseFail].toString)
    header.isSuccess should be(true)
    val wf = header.get
    wf.name should be("same_with_file_name")
    wf.options should be(Map())
    wf.timeout should be(0)
    wf.notifies.size should be(2)
    wf.notifies.head.recipients should be("a@q.com,b@q.com")
    wf.notifies.reverse.head.recipients should be("c@q.com,d@q.com")
  }

  it("parse sample un-ordered workflow") {
    val text =
      """
        |-- workflow=same_with_file_name
        |--  period=1440
        |--  loadType=incremental/full
        |--  logDrivenType=upstream/timewindow/diff/kafka/inc...
        |--  upstream=ods_product
        |
        |--  dependsOn=dim_user,dim_price
        |--  comment=this ETL script is working for xxx
        |--  timeout=3600
        |
        |--  defaultStart=20220101
        |--  stopScheduleWhenFail=false
        |--  options
        |--   project=abc
        |
        |--  notify
        |--   notifyType=email
        |--   recipients=a@q.com,b@q.com
        |--   notifyCondition=SUCCESS/FAILURE/ALWAYS
        |
        |
        |-- step=read csv
        |-- source=csv
        |-- target=csv
        |select 'a' as result;
        |
        |-- step=read csv
        |
        |-- source=csv
        |
        |-- target=csv
        |
        |WITH location AS (
        |    SELECT DISTINCT code
        |    FROM table1
        |    WHERE id  = (1,2,3)
        |      AND is_active = 1
        |)
        |select * from location;
        |
        |""".stripMargin

    val header = parse(text, workflow(_), verboseFailures = true)
    //print(header.asInstanceOf[Parsed.Failure].trace())
    header.isSuccess should be(true)
    val wf = header.get.value
    wf.name should be("same_with_file_name")
    wf.options.size should be(1)
    wf.notifies.head.recipients should be("a@q.com,b@q.com")
    wf.timeout should be(3600)
    wf.steps.size should be(2)
    wf.steps(0).sqlTemplate should be("select 'a' as result")
    wf.steps(1).sqlTemplate should be(
      """WITH location AS (
        |    SELECT DISTINCT code
        |    FROM table1
        |    WHERE id  = (1,2,3)
        |      AND is_active = 1
        |)
        |select * from location""".stripMargin)
  }


  it("parse sample step") {
    val text =
      """-- step=read csv
        |-- source=csv
        |--  fileDir=hdfs:///data/test/
        |--  filePaths=hdfs:///data/test/USER_${daily_postfix}.txt
        |--  sep=|
        |--  inferSchema=false
        |--  quote='
        |-- target=csv
        |--  fileDir=hdfs:///data/test/
        |--  filePaths=hdfs:///data/test/USER_${daily_postfix}.txt
        |--  sep=|
        |--  inferSchema=false
        |--  quote='
        |-- writeMode=overwrite
        |""".stripMargin

    val header = parse(text, step(_))
    header.isSuccess should be(true)
    val s = header.get.value
    s.sqlTemplate should be("")
    s.writeMode should be("overwrite")
  }


  it("parse complex step") {
    val text =
      """-- workflow=same_with_file_name
        |--  period=1440
        |--  loadType=incremental/full
        |--  logDrivenType=upstream/timewindow/diff/kafka/inc...
        |--  upstream=ods_product
        |
        |-- step=1
        |-- source=temp
        |
        |-- target=variables
        |select id, name,
        |
        |-- \\\\  -- this is another comment date=lates
        |       some_table;
        |
        |
        |
        |-- step=2
        |-- source=hive
        |-- target=temp
        |--  tableName=ads_si_view
        |-- throwExceptionIfEmpty=true
        |
        |-- \\\\  -- this is step2 sql
        |WITH location AS (
        |    SELECT DISTINCT code
        |    FROM table1
        |    WHERE id  = (1,2,3)
        |      AND is_active = 1
        |)
        |
        |-- \\\\  -- this is step2 select sql
        |
        |SELECT id,name,location
        |FROM table2
        |         LEFT OUTER JOIN supplier ON si.supplier_code = supplier.supplier_code
        |         LEFT OUTER JOIN cust ON si.cust_code = cust.oper_unit_code
        |         LEFT OUTER JOIN product ON SI.materiel_code = product.materiel_code
        |;
        |
        |""".stripMargin

    val header = parse(text, workflow(_))
    header.isSuccess should be(true)
    val wf = header.get.value
    wf.steps(0).sqlTemplate should be(
      """select id, name,
        |
        |-- \\\\  -- this is another comment date=lates
        |       some_table""".stripMargin)
    wf.steps(1).sqlTemplate should be(
      """-- \\\\  -- this is step2 sql
        |WITH location AS (
        |    SELECT DISTINCT code
        |    FROM table1
        |    WHERE id  = (1,2,3)
        |      AND is_active = 1
        |)
        |
        |-- \\\\  -- this is step2 select sql
        |
        |SELECT id,name,location
        |FROM table2
        |         LEFT OUTER JOIN supplier ON si.supplier_code = supplier.supplier_code
        |         LEFT OUTER JOIN cust ON si.cust_code = cust.oper_unit_code
        |         LEFT OUTER JOIN product ON SI.materiel_code = product.materiel_code
        |""".stripMargin)

  }

  it("parse options and conf") {
    val text =
      """
        |-- workflow=same_with_file_name
        |--  period=1440
        |--  loadType=incremental/full
        |--  logDrivenType=upstream/timewindow/diff/kafka/inc...
        |--  upstream=ods_product
        |
        |
        |-- step=1
        |-- source=temp
        |--  options
        |--   delimiter='
        |--   header=true
        |-- target=variables
        |--  options
        |--   delimiter="
        |--   header=false
        |-- conf
        |--  spark.sql.shuffle.partitions=1
        |select id, name,
        |
        |-- \\\\  -- this is another comment date=lates
        |       some_table;""".stripMargin

    val header = parse(text, workflow(_), verboseFailures = true)
    //print(header.asInstanceOf[Parsed.Failure].trace())
    header.isSuccess should be(true)
    val wf = header.get.value
    val steps = wf.steps

    assert(steps.length == 1)

    assert(steps(0).conf("spark.sql.shuffle.partitions") == "1")

    assert(steps(0).source.dataSourceType == "temp")
    assert(steps(0).source.options.size == 2)
    assert(steps(0).source.options("delimiter") == "'")
    assert(steps(0).source.options("header") == "true")

    assert(steps(0).target.dataSourceType == "variables")
    assert(steps(0).target.options.size == 2)
    assert(steps(0).target.options("delimiter") == "\"")
    assert(steps(0).target.options("header") == "false")
  }

  it("parse es options") {
    val text =
      """
        |-- workflow=same_with_file_name
        |--  period=1440
        |--  loadType=incremental/full
        |--  logDrivenType=upstream/timewindow/diff/kafka/inc...
        |--  upstream=ods_product
        |
        |
        |-- step=1
        |-- source=bigquery
        |--  options
        |--   delimiter='
        |--   queryTimeout=5
        |-- target=es
        |--  tableName=customer_label
        |--  primaryKeys=cifsn
        |--  options
        |--   es.mapping.parent=a=1;b=2;c=3
        |select id, name,
        |
        |-- \\\\  -- this is another comment date=lates
        |       some_table;""".stripMargin

    val header = parse(text, workflow(_), verboseFailures = true)
    //print(header.asInstanceOf[Parsed.Failure].trace())
    header.isSuccess should be(true)
    val wf = header.get.value
    val steps = wf.steps

    assert(steps.length == 1)

    val source = steps(0).source.asInstanceOf[BigQueryDataSourceConfig]
    assert(source.dataSourceType == "bigquery")
    assert(source.options.size == 2)
    assert(source.options("delimiter") == "'")
    assert(source.options("queryTimeout") == "5")

    val target = steps(0).target.asInstanceOf[DBDataSourceConfig]
    assert(target.dataSourceType == "es")
    assert(target.getPrimaryKeys == "cifsn")
    assert(target.options.size == 1)
    assert(target.options("es.mapping.parent") == "a=1;b=2;c=3")
  }

  it("parse steps without sql") {
    val text =
      """
        |-- workflow=same_with_file_name
        |--  period=1440
        |--  loadType=incremental/full
        |--  logDrivenType=upstream/timewindow/diff/kafka/inc...
        |--  upstream=ods_product
        |
        |
        |-- step=1
        |-- source=compresstar
        |--  encoding=utf-8
        |--  targetPath=/out/
        |--  tarPath=/out/((\w*.tar.gz))
        |--  tmpPath=/out/tmp/
        |--  bakPath=/out/bak/
        |--  fileNamePattern=\d{1}.txt
        |--  options
        |--   es.mapping.parent=a=1;b=2;c=3
        |-- target=do_nothing
        |--  options
        |--   options.parent=a=1;b=2;c=3
        |
        |-- step=2
        |-- source=csv
        |--  encoding=utf-8
        |--  inferSchema=true
        |--  sep=\t
        |--  header=false
        |--  fileNamePattern=\w*_1.txt
        |--  selectExpr=_c0 as num
        |--  fileDir=/out/
        |-- target=do_nothing
        |
        |-- step=3
        |-- source=csv
        |--  encoding=utf-8
        |--  inferSchema=true
        |--  sep=\t
        |--  header=false
        |--  fileNamePattern=\w*_1.txt
        |--  selectExpr=_c0 as num
        |--  fileDir=/out/
        |-- target=do_nothing
        |""".stripMargin

    val header = parse(text, workflow(_), verboseFailures = true)
    header match {
      case Parsed.Success(_, _) => ()
      case failure: Parsed.Failure => print(failure.trace())
    }
    header.isSuccess should be(true)
    val wf = header.get.value
    val steps = wf.steps

    assert(steps.length == 3)

    val source1 = steps(0).source.asInstanceOf[CompressTarConfig]
    assert(source1.dataSourceType == "compresstar")
    assert(source1.options.size == 1)
    assert(source1.options("es.mapping.parent") == "a=1;b=2;c=3")

    val target1 = steps(0).target.asInstanceOf[DBDataSourceConfig]
    assert(target1.dataSourceType == "do_nothing")
    assert(target1.options.size == 1)
    assert(target1.options("options.parent") == "a=1;b=2;c=3")


    val source2 = steps(1).source.asInstanceOf[CSVDataSourceConfig]
    assert(source2.dataSourceType == "csv")
    assert(source2.options.size == 0)

    val target2 = steps(1).target.asInstanceOf[DBDataSourceConfig]
    assert(target2.dataSourceType == "do_nothing")
    assert(target2.options.size == 0)

    val source3 = steps(2).source.asInstanceOf[CSVDataSourceConfig]
    assert(source3.dataSourceType == "csv")
    assert(source3.options.size == 0)

    val target3 = steps(2).target.asInstanceOf[DBDataSourceConfig]
    assert(target3.dataSourceType == "do_nothing")
    assert(target3.options.size == 0)
  }


  it("parse transformer args") {
    val text =
      """
        |-- workflow=same_with_file_name
        |--  period=1440
        |--  loadType=incremental/full
        |--  logDrivenType=upstream/timewindow/diff/kafka/inc...
        |--  upstream=ods_product
        |
        |
        |-- step=5
        |-- source=transformation
        |--  className=com.github.sharpdata.sharpetl.spark.transformation.SCDTransformer
        |--  methodName=transform
        |--   dwDataLoadType=full
        |--   dwViewName=`4e9e6a00`
        |--   odsViewName=`e7fb019e`
        |--   primaryFields=id
        |--   sortFields=id
        |--  transformerType=object
        |-- target=hive
        |--  dbName=default
        |--  tableName=test_cust
        |-- writeMode=overwrite
        |""".stripMargin

    val header = parse(text, workflow(_), verboseFailures = true)
    header match {
      case Parsed.Success(_, _) => ()
      case failure: Parsed.Failure => print(failure.trace())
    }
    header.isSuccess should be(true)
    val wf = header.get.value
    val steps = wf.steps

    assert(steps.length == 1)

    val source1 = steps(0).source.asInstanceOf[TransformationDataSourceConfig]
    assert(source1.dataSourceType == "transformation")
    assert(source1.methodName == "transform")
    assert(source1.transformerType == "object")
    assert(source1.args.size == 5)
    assert(source1.args("dwDataLoadType") == "full")
    assert(source1.args("dwViewName") == "`4e9e6a00`")
    assert(source1.args("odsViewName") == "`e7fb019e`")
    assert(source1.args("primaryFields") == "id")
    assert(source1.args("sortFields") == "id")
    assert(source1.options.size == 0)

    val target1 = steps(0).target.asInstanceOf[DBDataSourceConfig]
    target1.tableName should be("test_cust")
    target1.dbName should be("default")

    steps.head.writeMode should be("overwrite")
  }

  it("parse variable data source config") {
    val text =
      """
        |-- workflow=same_with_file_name
        |--  period=1440
        |--  loadType=incremental/full
        |--  logDrivenType=upstream/timewindow/diff/kafka/inc...
        |--  upstream=ods_product
        |
        |
        |-- step=generate variables（检查重复）
        |-- source=postgres
        |--  connectionName=my_postgres
        |--  dbName=pg_db
        |--  tableName=my_table
        |-- target=variables
        |select from_unixtime(unix_timestamp('2022-06-20 10:27:00', 'yyyy-MM-dd HH:mm:ss'), 'yyyy') as `YEAR`,
        |       from_unixtime(unix_timestamp('2022-06-20 10:27:00', 'yyyy-MM-dd HH:mm:ss'), 'MM')   as `MONTH`,
        |       from_unixtime(unix_timestamp('2022-06-20 10:27:00', 'yyyy-MM-dd HH:mm:ss'), 'dd')   as `DAY`,
        |       from_unixtime(unix_timestamp('2022-06-20 10:27:00', 'yyyy-MM-dd HH:mm:ss'), 'HH')   as `HOUR`;
        |
        |""".stripMargin

    val header = parse(text, workflow(_), verboseFailures = true)
    header match {
      case Parsed.Success(_, _) => ()
      case failure: Parsed.Failure => print(failure.trace())
    }
    header.isSuccess should be(true)
    val wf = header.get.value
    val steps = wf.steps

    assert(steps.length == 1)

    val source1 = steps(0).source.asInstanceOf[DBDataSourceConfig]
    assert(source1.dataSourceType == "postgres")
    assert(source1.connectionName == "my_postgres")
    assert(source1.dbName == "pg_db")
    assert(source1.tableName == "my_table")

    steps(0).target.asInstanceOf[VariableDataSourceConfig]
  }

  it("parse multi-line pair values") {
    val text =
      """--  connectionName=|
        |--  |Cats is a library which provides abstractions for functional programming in the Scala programming language.
        |--  |
        |--  |Scala supports both object-oriented and functional programming,
        |--  |and this is reflected in the hybrid approach of the standard library.
        |--  |Cats strives to provide functional programming abstractions that are core,
        |--  |binary compatible, modular, approachable and efficient.
        |--  |A broader goal of Cats is to provide a foundation for an ecosystem of pure,
        |--  |typeful libraries to support functional programming in Scala applications.
        |--  dbName=pg_db
        |--  tableName=my_table
        |""".stripMargin

    val header = parse(text, keyValPairs(2)(_), verboseFailures = true)
    header match {
      case Parsed.Success(_, _) => ()
      case failure: Parsed.Failure => print(failure.trace())
    }
    header.isSuccess should be(true)
    val wf = header.get.value
    wf.size should be(3)
    wf.head._2 should be(
      """Cats is a library which provides abstractions for functional programming in the Scala programming language.
        |
        |Scala supports both object-oriented and functional programming,
        |and this is reflected in the hybrid approach of the standard library.
        |Cats strives to provide functional programming abstractions that are core,
        |binary compatible, modular, approachable and efficient.
        |A broader goal of Cats is to provide a foundation for an ecosystem of pure,
        |typeful libraries to support functional programming in Scala applications.""".stripMargin)
  }

  it("parse nested multi-line pair value in workflow") {
    val text =
      """
        |-- workflow=same_with_file_name
        |--  period=1440
        |--  loadType=incremental/full
        |--  logDrivenType=upstream/timewindow/diff/kafka/inc...
        |--  upstream=ods_product
        |
        |
        |-- step=generate variables（检查重复）
        |-- source=postgres
        |--  connectionName=|
        |--  |Cats is a library which provides abstractions for functional programming in the Scala programming language.
        |--  |
        |--  |Scala supports both object-oriented and functional programming,
        |--  |and this is reflected in the hybrid approach of the standard library.
        |--  |Cats strives to provide functional programming abstractions that are core,
        |--  |binary compatible, modular, approachable and efficient.
        |--  |A broader goal of Cats is to provide a foundation for an ecosystem of pure,
        |--  |typeful libraries to support functional programming in Scala applications.
        |--  dbName=pg_db
        |--  tableName=my_table
        |--  options
        |--   es.mapping.parent=|
        |--   |[
        |--   |{"source_table": "a", "target_table": "a" },
        |--   |{"source_table": "b", "target_table": "b" }
        |--   |]
        |-- target=variables
        |select from_unixtime(unix_timestamp('2022-06-20 10:27:00', 'yyyy-MM-dd HH:mm:ss'), 'yyyy') as `YEAR`,
        |       from_unixtime(unix_timestamp('2022-06-20 10:27:00', 'yyyy-MM-dd HH:mm:ss'), 'MM')   as `MONTH`,
        |       from_unixtime(unix_timestamp('2022-06-20 10:27:00', 'yyyy-MM-dd HH:mm:ss'), 'dd')   as `DAY`,
        |       from_unixtime(unix_timestamp('2022-06-20 10:27:00', 'yyyy-MM-dd HH:mm:ss'), 'HH')   as `HOUR`;
        |
        |""".stripMargin

    val header = parse(text, workflow(_), verboseFailures = true)
    header match {
      case Parsed.Success(_, _) => ()
      case failure: Parsed.Failure => print(failure.trace())
    }
    header.isSuccess should be(true)
    val wf = header.get.value
    val steps = wf.steps

    assert(steps.length == 1)

    val source1 = steps(0).source.asInstanceOf[DBDataSourceConfig]
    assert(source1.dataSourceType == "postgres")
    assert(source1.connectionName ==
      """Cats is a library which provides abstractions for functional programming in the Scala programming language.
        |
        |Scala supports both object-oriented and functional programming,
        |and this is reflected in the hybrid approach of the standard library.
        |Cats strives to provide functional programming abstractions that are core,
        |binary compatible, modular, approachable and efficient.
        |A broader goal of Cats is to provide a foundation for an ecosystem of pure,
        |typeful libraries to support functional programming in Scala applications.""".stripMargin)
    assert(source1.dbName == "pg_db")
    assert(source1.tableName == "my_table")

    source1.options("es.mapping.parent") should be(
      """[
        |{"source_table": "a", "target_table": "a" },
        |{"source_table": "b", "target_table": "b" }
        |]""".stripMargin)
  }

  it("parse multi-line pair values in nestedObj") {
    val text =
      """--  options
        |--   es.mapping.parent=|
        |--   |[
        |--   |{"source_table": "a", "target_table": "a" },
        |--   |{"source_table": "b", "target_table": "b" }
        |--   |]
        |--  args
        |--   a=b
        |""".stripMargin

    val header = parse(text, options(2)(_), verboseFailures = true)
    header match {
      case Parsed.Success(_, _) => ()
      case failure: Parsed.Failure => print(failure.trace())
    }
    header.isSuccess should be(true)
    val wf = header.get.value
    wf.head._2 should be(
      """[
        |{"source_table": "a", "target_table": "a" },
        |{"source_table": "b", "target_table": "b" }
        |]""".stripMargin)
  }

  it("parse logDrivenType") {
    val text =
      """-- step=6
        |-- source=transformation
        |--  className=com.github.sharpdata.sharpetl.spark.transformation.JdbcLoadTransformer
        |--  methodName=transform
        |--   businessCreateTime=order_create_time
        |--   businessUpdateTime=order_update_time
        |--   currentDb=postgres
        |--   currentDbType=postgres
        |--   currentTable=dwd.t_fact_order
        |--   currentTableColumnsAndType=???
        |--   primaryFields=order_sn
        |--   slowChanging=false
        |--   updateTable=ods_t_order__target_selected
        |--   updateType=full
        |--  transformerType=object
        |-- target=do_nothing""".stripMargin

    val steplist = parse(text, steps(_)).get.value.toList

    val step = steplist.reverse.head

    step.toString should be(text + "\n")
  }

  it("parse single key value pair") {
    val text =
      """--  connectionName=a""".stripMargin

    val header = parse(text, keyValPairs(2)(_), verboseFailures = true)
    header match {
      case Parsed.Success(_, _) => ()
      case failure: Parsed.Failure => print(failure.trace())
    }
    header.isSuccess should be(true)
    val wf = header.get.value
    wf.size should be(1)
    wf.head._2 should be("a")
  }

  it("parse to temp data source if source not specified") {
    val wf1 =
      """-- workflow=default_to_temp_source
        |--  period=1440
        |--  loadType=incremental
        |--  logDrivenType=timewindow
        |
        |-- step=1
        |-- target=temp
        |--  tableName=temp_table
        |select 'SUCCESS' as `RESULT`;
        |
        |-- step=2
        |-- target=console
        |select * from temp_table;""".stripMargin


    val wf2 =
      """-- workflow=default_to_temp_source
        |--  period=1440
        |--  loadType=incremental
        |--  logDrivenType=timewindow
        |
        |-- step=1
        |-- source=temp
        |-- target=temp
        |--  tableName=temp_table
        |select 'SUCCESS' as `RESULT`;
        |
        |-- step=2
        |-- source=temp
        |-- target=console
        |select * from temp_table;""".stripMargin

    parseWorkflow(wf1).get.toString should be(parseWorkflow(wf2).get.toString)
  }

  it("parse loop over another table") {
    val wf =
      """-- workflow=default_to_temp_source
        |--  period=1440
        |--  loadType=incremental
        |--  logDrivenType=timewindow
        |
        |-- step=1
        |-- source=temp
        |-- target=temp
        |--  tableName=temp_table
        |select 'test_1' as `table_name`
        |union all
        |select 'test_2' as `table_name`
        |union all
        |select 'test_3' as `table_name`
        |union all
        |select 'test_4' as `table_name`
        |
        |-- step=2
        |-- source=http
        |--  url=http://localhost:1080/get_from_table/${table_name}
        |-- target=temp
        |--  tableName=target_temp_table
        |-- loopOver=temp_table""".stripMargin

    parseWorkflow(wf).get.steps(1).loopOver should be("temp_table")
  }
}
