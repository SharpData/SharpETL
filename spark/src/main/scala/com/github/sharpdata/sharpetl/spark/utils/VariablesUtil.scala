package com.github.sharpdata.sharpetl.spark.utils

import com.github.sharpdata.sharpetl.core.api.Variables
import com.github.sharpdata.sharpetl.core.util.ETLLogger
import org.apache.spark.sql.DataFrame

object VariablesUtil {

  def setVariables(
                    df: DataFrame,
                    variables: Variables): Unit = {
    if (!df.rdd.isEmpty) {
      val fieldNames = df.schema.fieldNames
      val row = df.first()
      fieldNames.foreach(fieldName => {
        val index = row.fieldIndex(fieldName)
        val fieldValue = if (row.isNullAt(index)) {
          "null"
        } else {
          row.get(index).toString
        }
        val key = if (fieldName.matches("^#\\{.+\\}$")) {
          fieldName
        } else {
          String.format("${%s}", fieldName)
        }
        variables += key -> fieldValue
      })
    }
    ETLLogger.info(s"Variables: $variables")
  }

}
