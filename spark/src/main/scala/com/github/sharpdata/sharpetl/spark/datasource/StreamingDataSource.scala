package com.github.sharpdata.sharpetl.spark.datasource

import com.github.sharpdata.sharpetl.datasource.kafka.KafkaConfig
import com.github.sharpdata.sharpetl.core.datasource.config.{DataSourceConfig, StreamingKafkaDataSourceConfig}
import com.github.sharpdata.sharpetl.core.exception.Exception.UnsupportedStreamingDataSourceException
import com.github.sharpdata.sharpetl.core.repository.model.JobLog
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import org.apache.spark.sql.Row
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object StreamingDataSource {
  def createDStream(step: WorkflowStep,
                    jobLog: JobLog,
                    streamingContext: StreamingContext
                    ): DStream[Row] = {
    val dataSourceConfig: DataSourceConfig = step.getSourceConfig
    dataSourceConfig match {
      case kafkaDataSourceConfig: StreamingKafkaDataSourceConfig =>
        KafkaDataSource.createDStream(
          streamingContext,
          kafkaDataSourceConfig
        )
      case _ =>
        throw UnsupportedStreamingDataSourceException(dataSourceConfig.dataSourceType)
    }
  }
}

private object KafkaDataSource {
  def createDStream(streamingContext: StreamingContext,
                    kafkaDataSourceConfig: StreamingKafkaDataSourceConfig): DStream[Row] = {
    val groupId = kafkaDataSourceConfig.getGroupId
    val topics = kafkaDataSourceConfig.getTopics.split(",").map(_.trim).toSet

    val kafkaParams: Map[String, Object] = KafkaConfig.buildNativeKafkaProducerConfig(groupId)
    KafkaUtils
      .createDirectStream[String, String](
        streamingContext,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams)
      )
      .map(consumerRecord => Row(consumerRecord.value()))
  }
}

