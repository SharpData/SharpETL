package com.github.sharpdata.sharpetl.core.datasource.config

import com.github.sharpdata.sharpetl.core.annotation.configFor
import com.github.sharpdata.sharpetl.core.datasource.config.KafkaDataFormat.JSON
import com.github.sharpdata.sharpetl.core.util.Constants.DataSourceType

import scala.beans.BeanProperty

sealed trait StreamingDataSourceConfig extends DataSourceConfig {
  // 微批执行间隔：秒
  @BeanProperty
  var interval: String = _
}

trait KafkaDataSourceConfig extends Serializable {
  // topic，多个 topic 逗号分隔
  @BeanProperty
  var topics: String = _

  @BeanProperty
  var groupId: String = _

  @BeanProperty
  var format: String = JSON

  // json 中需要解析的字段定义
  @BeanProperty
  var schemaDDL: String = _

  // topic 中需要获取的除value外其他的字段
  @BeanProperty
  var topicMessageColumns: String = _


  //是否开启序列化和反序列化
  @BeanProperty
  var enableSerDes: String = "true"
}

@configFor(types = Array("streaming_kafka"))
final class StreamingKafkaDataSourceConfig extends StreamingDataSourceConfig with KafkaDataSourceConfig

@configFor(types = Array("batch_kafka"))
final class BatchKafkaDataSourceConfig extends DataSourceConfig with KafkaDataSourceConfig

object KafkaDataFormat {
  val JSON: String = "JSON"
  val AVRO: String = "AVRO"
}


