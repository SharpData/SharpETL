package com.github.sharpdata.sharpetl.spark.end2end

import com.github.sharpdata.sharpetl.spark.end2end.mysql.MysqlSuitExecutor
import com.github.sharpdata.sharpetl.spark.end2end.postgres.PostgresSuitExecutor
import org.scalatest.Sequential

class SequentialExecutor extends
  Sequential(new PropertyLoadingSpec, new MysqlSuitExecutor, new PostgresSuitExecutor)
