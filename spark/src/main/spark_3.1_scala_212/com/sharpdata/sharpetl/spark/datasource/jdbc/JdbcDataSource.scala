package com.github.sharpdata.sharpetl.spark.datasoruce.jdbc

import org.apache.spark.TaskContext
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

object AbstractJdbcDataSource {
  def addTaskCompletionListener(close: () => Unit): TaskContext = {
    TaskContext.get().addTaskCompletionListener[Unit] { context: TaskContext => close() }
  }
}
