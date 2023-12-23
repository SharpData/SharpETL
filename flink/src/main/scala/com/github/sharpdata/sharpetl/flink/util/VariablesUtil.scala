package com.github.sharpdata.sharpetl.flink.util

import com.github.sharpdata.sharpetl.core.api.Variables
import com.github.sharpdata.sharpetl.core.util.ETLLogger
import com.github.sharpdata.sharpetl.flink.job.Types.DataFrame

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

object VariablesUtil {

  def setVariables(
                    df: DataFrame,
                    variables: Variables): Unit = {
    if (df.execute().collect().hasNext) {
      val fieldNames = df.getResolvedSchema.getColumns.map(_.getName)
      val row = df.execute().collect().next()
      fieldNames.zipWithIndex.foreach {
        case (fieldName, idx) =>
          val fieldValue = if (row.getField(idx).toString == "null") {
            "null"
          } else {
            row.getField(idx).toString
          }
          val key = if (fieldName.matches("^#\\{.+\\}$")) {
            fieldName
          } else {
            String.format("${%s}", fieldName)
          }
          variables += key -> fieldValue
      }
    }
    ETLLogger.info(s"Variables: $variables")
  }

}
