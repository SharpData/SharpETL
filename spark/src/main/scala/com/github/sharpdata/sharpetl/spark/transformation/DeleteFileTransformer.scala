package com.github.sharpdata.sharpetl.spark.transformation

import com.github.sharpdata.sharpetl.core.util.{ETLLogger, HDFSUtil}
import com.github.sharpdata.sharpetl.spark.utils.ETLSparkSession
import org.apache.spark.sql.DataFrame

// $COVERAGE-OFF$
object DeleteFileTransformer extends Transformer {

  override def transform(args: Map[String, String]): DataFrame = {
    val filePath = args("filePath")
    val fileNamePattern = args("fileNamePattern")
    ETLLogger.info(s"delete filePath:${filePath}")
    ETLLogger.info(s"delete fileNamePattern:${fileNamePattern}")
    HDFSUtil.listFileUrl(filePath, fileNamePattern).foreach(file => HDFSUtil.delete(file))
    ETLSparkSession.sparkSession.emptyDataFrame
  }
}
// $COVERAGE-ON$
