package com.github.sharpdata.sharpetl.datasource.kafka

import org.apache.spark.sql.DataFrame

object DFConversations {
  implicit class DataFrameConversations(df: DataFrame) {
    def isEmpty: Boolean = {
      df.limit(1).count() == 0
    }
  }
}
