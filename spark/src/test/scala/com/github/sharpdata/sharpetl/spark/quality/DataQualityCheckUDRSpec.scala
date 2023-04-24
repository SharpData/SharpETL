package com.github.sharpdata.sharpetl.spark.quality

import com.github.sharpdata.sharpetl.spark.job.{SparkSessionTestWrapper, SparkWorkflowInterpreter}
import com.github.sharpdata.sharpetl.spark.test.DatasetComparer
import com.github.sharpdata.sharpetl.core.datasource.config.DBDataSourceConfig
import com.github.sharpdata.sharpetl.core.quality.{DataQualityCheckResult, ErrorType, QualityCheckRule}
import com.github.sharpdata.sharpetl.core.repository.mysql.QualityCheckAccessor
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.spark.job.{SparkSessionTestWrapper, SparkWorkflowInterpreter}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should


class DataQualityCheckUDRSpec extends AnyFlatSpec with should.Matchers with SparkSessionTestWrapper with DatasetComparer {

  val rules = {
    Map(
      ("null check", QualityCheckRule("null check", "powerNullCheck($column)", ErrorType.error)),
      ("111 check", QualityCheckRule("111 check", "$column == \"111\" OR $column IS NULL", ErrorType.warn)),
      ("duplicated check", QualityCheckRule("duplicated check", "UDR.com.github.sharpdata.sharpetl.core.quality.udr.DuplicatedCheck", ErrorType.error)),
      ("233 agg check", QualityCheckRule("233 agg check", "UDR.com.github.sharpdata.sharpetl.spark.quality.udr.AggCheck", ErrorType.warn))
    )
  }
  val viewName = "test_view_name_udr"

  val interpreter = new SparkWorkflowInterpreterStub(spark, rules)

  it should "check duplicated value" in {

    val schema = List(
      StructField("id", IntegerType, true),
      StructField("phone", StringType, true),
      StructField("account", DoubleType, true),
      StructField("address", StringType, true)
    )

    val data = Seq(
      Row(1, "155 233 2333", 233.33, "beijing dongzhimen xxx number 23"),
      Row(1, "233 233 2334", 666.666, "beijing dongzhimen xxx number 23"),
      Row(3, null, 0.1, "xi'an tiangubalu xxx number 6"),
      Row(4, "155 233 2333", 123.456, "beijing dongzhimen xxx number 23")
    )

    val testDf = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      StructType(schema)
    )

    val step = new WorkflowStep()
    step.source = new DBDataSourceConfig()
    step.source.options = Map(
      ("idColumn", "id, phone"),
      ("topN", "2"),
      ("column.phone.qualityCheckRules", "duplicated check, null check"),
      ("column.address.qualityCheckRules", "duplicated check"),
      ("column.account.qualityCheckRules", "233 agg check")
    )

    //testDf.createOrReplaceTempView(viewName)
    //val sqlResult = interpreter.check(testDf, viewName, interpreter.parseQualityConfig(step), "id, phone", 2)
    //val udrResult = interpreter.checkUDR(testDf, viewName, interpreter.parseQualityConfig(step), "id, phone", 2)
    //val result = sqlResult union udrResult

    val result = interpreter.qualityCheck(step, "1", "???", testDf)

    val errors = result.error
    val warns = result.warn


    //    errors should be(
    //      Seq(
    //        DataQualityCheckResult("phone", "null check", "3__NULL", ErrorType.error, 0, 1),
    //        DataQualityCheckResult("phone", "duplicated check", "1__155 233 2333,4__155 233 2333", ErrorType.error, 0, 2),
    //        DataQualityCheckResult("address", "duplicated check", "1__155 233 2333,1__233 233 2334", ErrorType.error, 0, 3)
    //      )
    //    )

    errors.head should be(DataQualityCheckResult("phone", "null check", "3__NULL", ErrorType.error, 0, 1))

    errors(1).dataCheckType should be("duplicated check")
    errors(1).errorCount should be(2)
    errors(1).warnCount should be(0)

    errors(2).dataCheckType should be("duplicated check")
    errors(2).errorCount should be(3)
    errors(2).warnCount should be(0)

    Seq("1__155 233 2333", "4__155 233 2333") should contain theSameElementsAs errors(1).ids.split(",")
    errors(2).ids.split(",").toSet.subsetOf(
      Seq("1__155 233 2333", "1__233 233 2334", "4__155 233 2333").toSet
    ) should be(true)

    warns should be(
      Seq(
        DataQualityCheckResult("account", "233 agg check", "1__155 233 2333,1__233 233 2334", ErrorType.warn, 2, 0)
      )
    )

    val passed = result.passed.collectAsList()
    passed.isEmpty should be(true)
  }
}

final class SparkWorkflowInterpreterStub(override val spark: SparkSession,
                                         override val dataQualityCheckRules: Map[String, QualityCheckRule])
  extends SparkWorkflowInterpreter(spark, dataQualityCheckRules, new QualityCheckAccessor()) {
  override def recordCheckResult(jobId: String, jobScheduleId: String, results: Seq[DataQualityCheckResult]): Unit = ()
}

