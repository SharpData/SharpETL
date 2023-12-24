package com.github.sharpdata.sharpetl.spark.quality

import com.github.sharpdata.sharpetl.spark.job.SparkSessionTestWrapper
import com.github.sharpdata.sharpetl.spark.test.DatasetComparer
import com.github.sharpdata.sharpetl.core.quality.{DataQualityConfig, ErrorType, QualityCheckRule}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should


class DataQualityCheckRuleSpec extends AnyFlatSpec with should.Matchers with SparkSessionTestWrapper with DatasetComparer {

  val BUILT_IN_QUALITY_CHECK_RULES = Seq(
    QualityCheckRule("null check", "powerNullCheck($column)", ErrorType.error),
    QualityCheckRule("custom check for name and address", "powerNullCheck(name) AND powerNullCheck(address)", ErrorType.error)
  )

  it should "replace column placeholder with actual name" in {
    BUILT_IN_QUALITY_CHECK_RULES.head.withColumn("name") should be(
      DataQualityConfig("name", "null check", "powerNullCheck(`name`)", ErrorType.error)
    )
  }

  it should "make no effects with custom filter" in {
    BUILT_IN_QUALITY_CHECK_RULES.tail.head.withColumn("name") should be(
      DataQualityConfig("name", "custom check for name and address", "powerNullCheck(name) AND powerNullCheck(address)", ErrorType.error)
    )
  }
}