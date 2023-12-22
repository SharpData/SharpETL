package com.github.sharpdata.sharpetl.flink.job

import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.types.Row

object Types {
  type DataFrame = DataStream[Row]
}
