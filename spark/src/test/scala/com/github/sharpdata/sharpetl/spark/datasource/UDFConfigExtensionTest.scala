package com.github.sharpdata.sharpetl.spark.datasource

import com.github.sharpdata.sharpetl.spark.extension.UDFExtension
import com.github.sharpdata.sharpetl.spark.job.SparkSessionTestWrapper
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class UDFClass extends Serializable {
  def map_filter(map: Map[String, String], num: Int): Map[String, Int] = {
    map
      .map(t => t._1 -> t._2.toInt)
      .filter(_._2 > num)
  }
}

object UDFObj extends Serializable {
  def doNothing(): Int = {
    1
  }
}

class UDFConfigExtensionTest extends AnyFlatSpec with MockitoSugar with ArgumentMatchersSugar with SparkSessionTestWrapper with should.Matchers {


  it should "works with udf" in {
    try {
      UDFExtension.registerUDF(
        spark,
        "class",
        "map_filter",
        "com.github.sharpdata.sharpetl.spark.datasource.UDFClass",
        "map_filter"
      )
      UDFExtension.registerUDF(
        spark,
        "object",
        "do_nothing",
        "com.github.sharpdata.sharpetl.spark.datasource.UDFObj",
        "doNothing"
      )
      val sql =
        """
          |select map_filter(str_to_map('a:1,b:1,c:0', ',', ':'), 0),
          |       do_nothing()
          |""".stripMargin
      spark
        .sql(sql)
        .show()
    }

  }
}