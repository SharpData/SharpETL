package com.github.sharpdata.sharpetl.spark.end2end

import com.github.sharpdata.sharpetl.spark.end2end.postgres.PostgresSuitExecutor
import org.scalatest.Sequential

class SequentialExecutor extends
  Sequential(new PropertyLoadingSpec, new PostgresSuitExecutor, new ETLSuitExecutor)
