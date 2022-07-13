package com.github.sharpdata.sharpetl.spark.common

import com.github.sharpdata.sharpetl.core.util.Constants.Pattern
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class PatternTest extends AnyFunSpec {
  private val UNKNOWN = -1
  private val NUM_PARTITIONS = 1
  private val REPARTITION_COLUMNS = 2
  private val NUM_PARTITIONS_AND_REPARTITION_COLUMNS = 3

  describe("Repartition pattern") {
    it("should return the correct match result") {
      matchRepartitionArgsType("0") should be(UNKNOWN)
      matchRepartitionArgsType("1,") should be(UNKNOWN)
      matchRepartitionArgsType("1") should be(NUM_PARTITIONS)
      matchRepartitionArgsType("10") should be(NUM_PARTITIONS)

      matchRepartitionArgsType("1a") should be(UNKNOWN)
      matchRepartitionArgsType("_a") should be(REPARTITION_COLUMNS)
      matchRepartitionArgsType("a") should be(REPARTITION_COLUMNS)
      matchRepartitionArgsType("a1_") should be(REPARTITION_COLUMNS)
      matchRepartitionArgsType("a1_,B_2") should be(REPARTITION_COLUMNS)

      matchRepartitionArgsType("0,a1_,B_2") should be(UNKNOWN)
      matchRepartitionArgsType("10,a1_,B_2") should be(NUM_PARTITIONS_AND_REPARTITION_COLUMNS)
    }
  }

  def matchRepartitionArgsType(repartitionArgs: String): Int = {
    repartitionArgs match {
      case Pattern.REPARTITION_NUM_PATTERN() =>
        NUM_PARTITIONS
      case Pattern.REPARTITION_COLUMNS_PATTERN(_) =>
        REPARTITION_COLUMNS
      case Pattern.REPARTITION_NUM_COLUMNS_PATTERN(_) =>
        NUM_PARTITIONS_AND_REPARTITION_COLUMNS
      case _ =>
        UNKNOWN
    }
  }
}
