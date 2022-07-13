package com.github.sharpdata.sharpetl.spark.transformation

import com.github.sharpdata.sharpetl.core.util.Constants.Environment
import com.github.sharpdata.sharpetl.core.util.ETLLogger
import com.github.sharpdata.sharpetl.spark.utils.ETLSparkSession.sparkSession
import com.github.sharpdata.sharpetl.datasource.kafka.KafkaConfig.{buildNativeKafkaConsumerConfig, buildNativeKafkaProducerConfig}
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.sql.DataFrame

import java.lang
import scala.annotation.tailrec
import scala.collection.mutable
import scala.jdk.CollectionConverters._

// $COVERAGE-OFF$
object EnsureSinkConnectorFinished extends Transformer {

  final case class WaitTimeoutException(msg: String) extends RuntimeException(msg)

  /**
   * make sure all data in topic already sink into HDFS by check consumer-group lag
   *
   * command: `kafka-consumer-groups --group consumer-group --describe --bootstrap-server localhost:9092 --command-config /opt/kafka_cfg/client.properties`
   */
  override def transform(args: Map[String, String]): DataFrame = {
    val consumerGroup = args("group")
    val kafkaTopic = args.getOrElse("kafkaTopic", "")
    val propertyPrefix = args.getOrElse("propertyPrefix", "kafka.producer")
    val consumerConfig = propertyPrefix match {
      case "kafka.consumer" => buildNativeKafkaConsumerConfig(consumerGroup).asJava
      case "kafka.producer" => buildNativeKafkaProducerConfig(consumerGroup).asJava
    }
    val kafkaClient = new KafkaConsumer(consumerConfig)
    val adminClient = AdminClient.create(consumerConfig)

    @tailrec def waitUntilNoLag(loopTimes: Int = 0): Unit = {
      val lag = getLag(consumerGroup, kafkaClient, adminClient, kafkaTopic)
      if (lag == 0) {
        ETLLogger.info(s"Current lag is $lag, check passed")
      } else if (loopTimes > 10) {
        throw WaitTimeoutException(s"Timeout waiting for 0 lag, current lag is $lag")
      } else {
        ETLLogger.warn(s"Current lag is $lag, wait for 30 seconds...")
        Thread.sleep(30 * 1000)
        waitUntilNoLag(loopTimes + 1)
      }
    }

    try {
      waitUntilNoLag()
    } finally {
      kafkaClient.close()
      adminClient.close()
    }

    sparkSession.emptyDataFrame
  }

  private def getLag(consumerGroup: String, kafkaClient: KafkaConsumer[Nothing, Nothing], adminClient: AdminClient, kafkaTopic: String) = {
    val consumedOffsets = adminClient.listConsumerGroupOffsets(consumerGroup).partitionsToOffsetAndMetadata().get()
    val endOffsets = kafkaClient.endOffsets(consumedOffsets.keySet)

    val consumedOffsetWithTopic : mutable.Map[TopicPartition, OffsetAndMetadata]= kafkaTopic match {
      case "" => consumedOffsets.asScala
      case _ => consumedOffsets.asScala.filter(it => it._1.topic == kafkaTopic)
    }

    if (Environment.current == Environment.PROD) {
      assert(consumedOffsetWithTopic.nonEmpty)
    }

    val endOffsetsWithTopic : mutable.Map[TopicPartition, lang.Long]= kafkaTopic match {
      case "" => endOffsets.asScala
      case _ => endOffsets.asScala.filter(it => it._1.topic == kafkaTopic)
    }

    val endOffset = endOffsetsWithTopic.values.map(_.toLong).sum
    val consumedOffset = consumedOffsetWithTopic.values.map(_.offset()).sum

    if (Environment.current == Environment.PROD) {
      assert(endOffset > 0, "The end offset can not be 0, which means there are no data in the topic")
      assert(consumedOffset > 0, "The consumed offset can not be 0, which means the consumer never consumed any data from topic")
    }
    ETLLogger.info(s"End offset $endOffset, consumed offset $consumedOffset")
    endOffset - consumedOffset
  }
}
// $COVERAGE-ON$
