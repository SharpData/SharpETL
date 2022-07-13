package com.github.sharpdata.sharpetl.datasource.es

import com.github.sharpdata.sharpetl.core.util.ETLConfig
import org.apache.commons.lang3.StringUtils
import org.elasticsearch.hadoop.cfg.ConfigurationOptions

object EsConfig {

  def buildConfig(mappingId: String = ""): Map[String, String] = {
    if (StringUtils.isNotEmpty(mappingId)) {
      // update 模式必须设置 es.mapping.id
      defaultConfig += ConfigurationOptions.ES_MAPPING_ID -> mappingId
    }
    defaultConfig
  }

  private var defaultConfig: Map[String, String] = {
    Map(
      // hosts
      ConfigurationOptions.ES_NODES -> ETLConfig.getProperty("es.nodes"),
      // userName
      ConfigurationOptions.ES_NET_HTTP_AUTH_USER -> ETLConfig.getProperty("es.net.http.auth.user"),
      // password
      ConfigurationOptions.ES_NET_HTTP_AUTH_PASS -> ETLConfig.getProperty("es.net.http.auth.pass"),
      // 是否在进行批处理写入后触发索引刷新，只有在执行了整个写入（意味着多个批量更新）之后才会调用此方法，默认 true
      ConfigurationOptions.ES_BATCH_WRITE_REFRESH -> ETLConfig.getProperty("es.batch.write.refresh"),
      // 是否自动创建 index，默认 true
      ConfigurationOptions.ES_INDEX_AUTO_CREATE -> ETLConfig.getProperty("es.index.auto.create"),
      // 默认 1000
      ConfigurationOptions.ES_BATCH_SIZE_ENTRIES -> ETLConfig.getProperty("es.batch.size.entries"),
      // 默认 1mb
      ConfigurationOptions.ES_BATCH_SIZE_BYTES -> ETLConfig.getProperty("es.batch.size.bytes"),
      // 默认为 index，共五种输出模式：index、create、update、upsert、delete
      ConfigurationOptions.ES_WRITE_OPERATION -> ETLConfig.getProperty("es.write.operation"),
      // 字段值为 null 时是否写入此字段，默认为 false
      ConfigurationOptions.ES_SPARK_DATAFRAME_WRITE_NULL_VALUES -> ETLConfig.getProperty("es.spark.dataframe.write.null"),
      // 默认 false
      ConfigurationOptions.ES_NODES_WAN_ONLY -> ETLConfig.getProperty("es.nodes.wan.only")
    )
  }

}
