package com.github.sharpdata.sharpetl.spark.quality

import com.github.sharpdata.sharpetl.spark.job.{SparkSessionTestWrapper, SparkWorkflowInterpreter}
import com.github.sharpdata.sharpetl.spark.test.DatasetComparer
import com.github.sharpdata.sharpetl.core.datasource.config.DBDataSourceConfig
import com.github.sharpdata.sharpetl.core.repository.mysql.QualityCheckAccessor
import com.github.sharpdata.sharpetl.core.quality.{DataQualityCheckResult, ErrorType, QualityCheck, QualityCheckRule}
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.core.util.StringUtil
import com.github.sharpdata.sharpetl.spark.job.{SparkSessionTestWrapper, SparkWorkflowInterpreter}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should


class DataQualityCheckSpec extends AnyFlatSpec with should.Matchers with SparkSessionTestWrapper with DatasetComparer with BeforeAndAfterEach {

  val rules = {
    Map(
      ("null check", QualityCheckRule("null check", "powerNullCheck($column)", ErrorType.error)),
      ("111 check", QualityCheckRule("111 check", "$column == \"111\" OR $column IS NULL", ErrorType.warn))
    )
  }
  val qualityCheckAccessor = new QualityCheckAccessor()

  val interpreter = new SparkWorkflowInterpreter(spark, rules, qualityCheckAccessor)

  val viewName = "test_view_name"

  val resultView = s"${viewName}_result"

  override protected def afterEach(): Unit = {
    spark.catalog.dropTempView(viewName)
    spark.catalog.dropTempView(resultView)
    super.afterEach()
  }

  it should "check simple null data" in {

    val schema = List(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true)
    )

    val data = Seq(
      Row(1, "111"),
      Row(2, "222"),
      Row(3, null),
      Row(4, null)
    )

    val testDf = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      StructType(schema)
    )

    val step = new WorkflowStep()
    step.source = new DBDataSourceConfig()
    step.source.options = Map(
      ("idColumn", "id"),
      ("column.name.qualityCheckRules", "null check")
    )
    testDf.createOrReplaceTempView(viewName)
    val result = interpreter.check(viewName, interpreter.parseQualityConfig(step), "id").error

    result.size should be(1)
    result.head.ids should be("3,4")
  }

  it should "check simple null string" in {

    val schema = List(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true)
    )

    val data = Seq(
      Row(1, "111"),
      Row(2, "222"),
      Row(3, "null"),
      Row(4, null)
    )

    val testDf = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      StructType(schema)
    )

    val step = new WorkflowStep()
    step.source = new DBDataSourceConfig()
    step.source.options = Map(
      ("idColumn", "id"),
      ("column.name.qualityCheckRules", "null check")
    )
    testDf.createOrReplaceTempView(viewName)
    val result = interpreter.check(viewName, interpreter.parseQualityConfig(step), "id").error

    result.size should be(1)
    result.head.ids should be("3,4")
  }

  it should "support UDF `powerNullCheck` " in {

    val schema = List(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true)
    )

    val data = Seq(
      Row(1, "111"),
      Row(2, "222"),
      Row(3, "null"),
      Row(4, null),
      Row(5, "nUlL")
    )

    val testDf = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      StructType(schema)
    )

    val step = new WorkflowStep()
    step.source = new DBDataSourceConfig()
    step.source.options = Map(
      ("idColumn", "id"),
      ("column.name.qualityCheckRules", "null check")
    )
    testDf.createOrReplaceTempView(viewName)
    val result = interpreter.check(viewName, interpreter.parseQualityConfig(step), "id").error


    result.size should be(1)
    result.head.ids should be("3,4,5")
  }

  it should "run multiple rules in one query" in {

    val schema = List(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true)
    )

    val data = Seq(
      Row(1, "111"),
      Row(2, "222"),
      Row(3, "null"),
      Row(4, null),
      Row(5, "nUlL")
    )

    val testDf = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      StructType(schema)
    )

    val step = new WorkflowStep()
    step.source = new DBDataSourceConfig()
    step.source.options = Map(
      ("idColumn", "id"),
      ("column.name.qualityCheckRules", "null check,          111 check      ")
    )

    val interpreter = new SparkWorkflowInterpreter(spark,
      Map(
        ("null check", QualityCheckRule("null check", "powerNullCheck($column)", ErrorType.error)),
        ("111 check", QualityCheckRule("111 check", "$column == \"111\" OR $column IS NULL", ErrorType.error))
      ),
      qualityCheckAccessor
    )
    testDf.createOrReplaceTempView(viewName)
    val result = interpreter.check(viewName, interpreter.parseQualityConfig(step), "id")

    val errors = result.error

    errors.size should be(2)
    errors.head.ids should be("3,4,5")
    errors.head.dataCheckType should be("null check")
    errors.tail.head.ids should be("1,4")
    errors.tail.head.dataCheckType should be("111 check")

    result.warn.size should be(0)
    val passed = passedResult(result.sql, viewName, resultView).collectAsList()
    passed.size() should be(1)
  }

  it should "group by column name, check type" in {

    val schema = List(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("address", StringType, true)
    )

    val data = Seq(
      Row(1, "111", " Null "),
      Row(2, "222", "Beijing"),
      Row(3, "null", "Xian"),
      Row(4, null, "         null"),
      Row(5, "nUlL", "ShangHai")
    )

    val testDf = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      StructType(schema)
    )


    val step = new WorkflowStep()
    step.source = new DBDataSourceConfig()
    step.source.options = Map(
      ("idColumn", "id"),
      ("column.name.qualityCheckRules", "null check,          111 check      "),
      ("column.address.qualityCheckRules", "null check")
    )
    testDf.createOrReplaceTempView(viewName)
    val result = interpreter.check(viewName, interpreter.parseQualityConfig(step), "id")

    val errors = result.error
    val warns = result.warn

    errors should be(
      Seq(
        DataQualityCheckResult("name", "null check", "3,4,5", ErrorType.error, 0, 3),
        DataQualityCheckResult("address", "null check", "1,4", ErrorType.error, 0, 2)
      )
    )

    warns should be(
      Seq(
        DataQualityCheckResult("name", "111 check", "1,4", ErrorType.warn, 2, 0)
      )
    )

    val passed = passedResult(result.sql, viewName, resultView).collectAsList()
    passed.size() should be(1)
  }

  it should "limit works" in {

    val schema = List(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("address", StringType, true)
    )

    val data = Seq(
      Row(1, "111", " Null "),
      Row(2, "222", "Beijing"),
      Row(3, "null", "Xian"),
      Row(4, null, "         null"),
      Row(5, "nUlL", "ShangHai")
    )

    val testDf = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      StructType(schema)
    )

    val step = new WorkflowStep()
    step.source = new DBDataSourceConfig()
    step.source.options = Map(
      ("idColumn", "id"),
      ("column.name.qualityCheckRules", "null check,          111 check      "),
      ("column.address.qualityCheckRules", "null check")
    )
    testDf.createOrReplaceTempView(viewName)
    val result = interpreter.check(viewName, interpreter.parseQualityConfig(step), "id", 2)

    val errors = result.error
    val warns = result.warn

    errors should be(
      Seq(
        DataQualityCheckResult("name", "null check", "3,4", ErrorType.error, 0, 3),
        DataQualityCheckResult("address", "null check", "1,4", ErrorType.error, 0, 2)
      )
    )

    warns should be(
      Seq(
        DataQualityCheckResult("name", "111 check", "1,4", ErrorType.warn, 2, 0)
      )
    )

    val passed = passedResult(result.sql, viewName, resultView).collectAsList()
    passed.size() should be(1)
  }

  it should "support multiple column ids" in {

    val schema = List(
      StructField("id", IntegerType, true),
      StructField("phone", StringType, true),
      StructField("name", StringType, true),
      StructField("address", StringType, true)
    )

    val data = Seq(
      Row(1, "155 233 2333", "111", " Null "),
      Row(1, "233 233 2333", "222", "null        "),
      Row(3, "155 233 2333", "null", "Xian"),
      Row(4, "155 233 2333", null, "         null"),
      Row(5, "155 233 2333", "nUlL", "ShangHai"),
      Row(6, "155 233 2333", "zhang san", "ShangHai")
    )

    val testDf = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      StructType(schema)
    )

    val step = new WorkflowStep()
    step.source = new DBDataSourceConfig()
    step.source.options = Map(
      ("idColumn", "id"),
      ("column.name.qualityCheckRules", "null check,          111 check      "),
      ("column.address.qualityCheckRules", "null check")
    )
    testDf.createOrReplaceTempView(viewName)
    val result = interpreter.check(viewName, interpreter.parseQualityConfig(step), "id, phone", 2)

    val errors = result.error
    val warns = result.warn

    errors should be(
      Seq(
        DataQualityCheckResult("name", "null check", "3__155 233 2333,4__155 233 2333", ErrorType.error, 0, 3),
        DataQualityCheckResult("address", "null check", "1__155 233 2333,1__233 233 2333", ErrorType.error, 0, 3)
      )
    )

    warns should be(
      Seq(
        DataQualityCheckResult("name", "111 check", "1__155 233 2333,4__155 233 2333", ErrorType.warn, 2, 0)
      )
    )

    val passed = passedResult(result.sql, viewName, resultView).collectAsList()
    passed.size() should be(1)
  }

  it should "support multiple column ids drop duplicated data" in {

    val schema = List(
      StructField("id", IntegerType, true),
      StructField("phone", StringType, true),
      StructField("name", StringType, true),
      StructField("address", StringType, true)
    )

    val data = Seq(
      Row(1, "155 233 2333", "111", " Null "),
      Row(1, "155 233 2333", "222", "null        "),
      Row(3, "155 233 2333", "lisi", "Xian"),
      Row(4, "155 233 2333", null, "         null"),
      Row(5, "155 233 2333", "wangwu", "ShangHai"),
      Row(5, "155 233 2333", "zhang san", "ShangHai")
    )

    val testDf = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      StructType(schema)
    )

    val step = new WorkflowStep()
    step.source = new DBDataSourceConfig()
    step.source.options = Map(
      ("idColumn", "id, phone"),
      ("column.name.qualityCheckRules", "null check")
    )
    testDf.createOrReplaceTempView(viewName)

    val interpreter = new SparkWorkflowInterpreterStub(spark, rules)

    val result = interpreter.qualityCheck(step, 1, "???", testDf)


    val errors = result.error

    errors should be(
      Seq(
        DataQualityCheckResult("name", "null check", "4__155 233 2333", ErrorType.error, 0, 1),
        DataQualityCheckResult("id, phone", "Duplicated PK check", "1__155 233 2333,5__155 233 2333", ErrorType.error, 0, 2)
      )
    )

    //result.passed.show(truncate = false)

    result.passed.collectAsList().size() should be(3)
  }

  def passedResult(sql: String, tempViewName: String, resultView: String): DataFrame = {
    val df = spark.sql(QualityCheck.generateAntiJoinSql(sql, StringUtil.EMPTY, tempViewName))
    spark.catalog.dropTempView(tempViewName)
    spark.catalog.dropTempView(resultView)
    df
  }
}
