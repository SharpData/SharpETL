package com.github.sharpdata.sharpetl.spark.datasource

import com.github.sharpdata.sharpetl.core.util.HDFSUtil
import org.scalatest.funspec.AnyFunSpec

class HdfsDataSourceTest  extends AnyFunSpec {

  ignore("Hdfs Data Source") {
    it("should list all task files") {
      val taskPath = "hdfs:/user/hive/data-pipeline/tasks/"
      val tasks = HDFSUtil.recursiveListFiles(taskPath)
      assert(tasks.nonEmpty)
    }
  }

}
