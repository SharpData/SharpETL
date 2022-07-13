package com.github.sharpdata.sharpetl.datasource.kafka

import com.github.sharpdata.sharpetl.core.exception.Exception.MissingConfigurationException
import com.github.sharpdata.sharpetl.core.util.{ETLConfig, StringUtil}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.types.StructType

import scala.jdk.CollectionConverters._
import java.util.Locale


object KafkaConfig {

  private lazy val defaultStructuredStreamingConfig: Map[String, String] = {
    val props = kafkaProps("consumer")
    val mustHaveConf = Set("bootstrap.servers", "startingOffsets", "failOnDataLoss", "zookeeper.connect")
    if (!mustHaveConf.subsetOf(props.keySet)) {
      throw MissingConfigurationException(s"Kafka prop is missing, ${mustHaveConf.map("kafka." + _).mkString(",")}" +
        s" must present in application-{env}.properties file.")
    }
    props
  }

  def buildStructuredStreamingConfig(topics: String): Map[String, String] = {
    Map(
      "subscribe" -> topics
    ) ++ defaultStructuredStreamingConfig
  }

  def buildNativeKafkaProducerConfig(groupId: String): Map[String, Object] = {
    Map(
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId
    ) ++ nativeKafkaProps("producer")
  }

  def buildNativeKafkaConsumerConfig(groupId: String): Map[String, Object] = {
    Map(
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId
    ) ++ nativeKafkaProps("consumer")
  }

  def buildSparkKafkaConsumerConfig(groupId: String, topics: String): Map[String, String] = {
    Map(
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> groupId,
      "subscribe" -> topics
    ) ++ kafkaProps("consumer")
  }

  def buildSparkKafkaProducerConfig(topics: String): Map[String, String] = {
    Map(
      "topic" -> topics
    ) ++ kafkaProps("producer")
  }

  def kafkaProps(prop: String): Map[String, String] = {
    val prefix = s"kafka.$prop."
    ETLConfig.plainProperties.filterKeys(_.startsWith(prefix))
      .map { case (k, v) => (k.replaceFirst(prefix, ""), v) }
      .toMap
  }

  // copy from org.apache.spark.sql.kafka010.KafkaSourceProvider.createSource
  def nativeKafkaProps(prop: String): Map[String, String] = {
    val parameters = kafkaProps(prop)
    parameters
      .keySet
      .filter(_.toLowerCase(Locale.ROOT).startsWith("kafka."))
      .map { k => k.drop(6) -> parameters(k) }
      .toMap

  }

  def schemaMapping: StructType => Seq[String] = {
    schema =>
      schema
        .fieldNames
        .map(fieldName => s"""data.$fieldName as ${StringUtil.humpToUnderline(fieldName)}""")
  }
}
