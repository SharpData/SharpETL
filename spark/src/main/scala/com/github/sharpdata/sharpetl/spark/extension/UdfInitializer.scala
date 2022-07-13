package com.github.sharpdata.sharpetl.spark.extension

import com.github.sharpdata.sharpetl.core.extension.BuiltInFunctions
import com.github.sharpdata.sharpetl.core.util.Constants.DataSourceType
import org.apache.spark.sql.SparkSession

object UdfInitializer extends Serializable {
  /**
   * Init built-in UDFs from [[BuiltInFunctions]]
   */
  lazy val init = { (spark: SparkSession) =>
    Seq("powerNullCheck", "arrayJoin", "top", "flatten", "ifEmpty")
      .foreach(func => {
        UDFExtension.registerUDF(
          spark,
          DataSourceType.OBJECT,
          func,
          "com.github.sharpdata.sharpetl.core.extension.BuiltInFunctions",
          func
        )
      })
  }
}
