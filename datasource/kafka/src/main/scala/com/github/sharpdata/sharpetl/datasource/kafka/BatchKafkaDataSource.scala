package com.github.sharpdata.sharpetl.datasource.kafka

import com.github.sharpdata.sharpetl.core.annotation._
import com.google.common.base.Strings.isNullOrEmpty
import com.github.sharpdata.sharpetl.core.datasource.config.BatchKafkaDataSourceConfig
import com.github.sharpdata.sharpetl.core.datasource.config.KafkaDataFormat.{AVRO, JSON}
import com.github.sharpdata.sharpetl.core.repository.model.JobLog
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.core.util.ETLLogger
import KafkaConfig.{buildSparkKafkaConsumerConfig, buildSparkKafkaProducerConfig, schemaMapping}
import OffsetRange.offsetEncoder
import org.apache.spark.sql.functions.{col, from_json, lit, struct, to_json}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import DFConversations._
import com.github.sharpdata.sharpetl.core.api.Variables
import com.github.sharpdata.sharpetl.core.datasource.{Sink, Source}

import java.util.UUID
import scala.jdk.CollectionConverters._

@source(types = Array("batch_kafka"))
@sink(types = Array("batch_kafka"))
class BatchKafkaDataSource extends Source[DataFrame, SparkSession] with Sink[DataFrame] {
  override def read(step: WorkflowStep, jobLog: JobLog, executionContext: SparkSession, variables: Variables): DataFrame = {
    import executionContext.implicits._
    val kafkaDataSourceConfig = step.source.asInstanceOf[BatchKafkaDataSourceConfig]

    ETLLogger.info(
      s"""
         |Start processing data of topics ${kafkaDataSourceConfig.topics}
         |with startingOffsets ${jobLog.dataRangeStart} and
         |endingOffsets ${jobLog.dataRangeEnd}
         |""".stripMargin)

    val kafkaProps = buildSparkKafkaConsumerConfig(kafkaDataSourceConfig.groupId, kafkaDataSourceConfig.topics)

    val sourceSchema: StructType = StructType.fromDDL(kafkaDataSourceConfig.getSchemaDDL)
    val schemaMappingExpr = schemaMapping(sourceSchema)

    kafkaDataSourceConfig.format match {
      case JSON =>
        val originDf = executionContext
          .read
          .format("kafka")
          .option("startingOffsets", jobLog.dataRangeStart)
          .option("endingOffsets", jobLog.dataRangeEnd) //useful when re-run old job
          .options(kafkaProps)
          .load()

        if (originDf.isEmpty) {
          ETLLogger.warn(s"There are no new data need to be processed, set dataRangeEnd to ${jobLog.dataRangeStart}")
          jobLog.dataRangeEnd = jobLog.dataRangeStart
        } else if (jobLog.dataRangeEnd != "latest") {
          () // do nothing when re-run topic based job
        } else {
          val tempViewName = s"`${UUID.randomUUID().toString.split("-").head}`"

          originDf.createOrReplaceTempView(tempViewName)

          jobLog.dataRangeEnd =
            executionContext.sql(
              s"""
                 |select `topic`,`partition`, max(offset) + 1 as maxOffset
                 |from $tempViewName group by `topic`,`partition`""".stripMargin)
              .as[OffsetRange](offsetEncoder)
              .collectAsList()
              .asScala
              .toList
              .asEndJson
          ETLLogger.info(s"Newer data polled from kafka topic ${kafkaDataSourceConfig.topics}, update endingOffsets to ${jobLog.dataRangeEnd}")
        }

        val messageColumnNames = kafkaDataSourceConfig.topicMessageColumns match {
          case value: String if !isNullOrEmpty(value) => kafkaDataSourceConfig.topicMessageColumns.split(",").map(_.trim)
          case _ => Array.empty[String]
        }
        val exprColumns = "CAST(value as STRING)" +: messageColumnNames.map(it => s"CAST($it as STRING)")
        val selectColumns = (from_json($"value", sourceSchema) as "data") +: messageColumnNames.map(col)
        val allSchemaMappingExpr = schemaMappingExpr ++ messageColumnNames.toSeq
        originDf.selectExpr(exprColumns: _*).select(selectColumns: _*).selectExpr(allSchemaMappingExpr: _*)

      case AVRO => ???
      case _ => ???
    }
  }

  override def sink(df: DataFrame, step: WorkflowStep, variables: Variables): Unit = {
    val kafkaDataSourceConfig = step.target.asInstanceOf[BatchKafkaDataSourceConfig]

    {
      if (kafkaDataSourceConfig.enableSerDes.toLowerCase().toBoolean) {
        df.select(lit(""), to_json(struct("*")))
      } else {
        df.select(lit(""), struct("json"))
      }
    }.toDF("key", "value")
      .selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value")
      .write
      .format("kafka")
      .options(buildSparkKafkaProducerConfig(kafkaDataSourceConfig.topics))
      .save()
  }
}
