package com.github.sharpdata.sharpetl.spark.job

import com.github.sharpdata.sharpetl.core.datasource.config.{StreamingDataSourceConfig, StreamingKafkaDataSourceConfig}
import com.github.sharpdata.sharpetl.spark.utils.ETLSparkSession.sparkSession
import com.github.sharpdata.sharpetl.spark.utils.ETLSparkSession.sparkSession.implicits._
import com.github.sharpdata.sharpetl.datasource.kafka.KafkaConfig.schemaMapping
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.dstream.DStream


object StreamingStep {
  def executeStreamingStep(streamingDataSourceConfig : StreamingDataSourceConfig,
                           stream: DStream[Row],
                          streamingCallback: DataFrame => Unit
                          ) : Unit = {
    streamingDataSourceConfig match {
      case _ => KafkaStreamingStep.execute(streamingDataSourceConfig, stream, streamingCallback)

    }
  }
}

object KafkaStreamingStep{

  def execute( streamingDataSourceConfig : StreamingDataSourceConfig,
               stream: DStream[Row],
               streamingCallback: DataFrame => Unit): Unit = {
    //assume the rest streaming data source always be [[StreamKafkaDataSourceConfig]]
    val sourceSchema: StructType = StructType.fromDDL(streamingDataSourceConfig.asInstanceOf[StreamingKafkaDataSourceConfig].getSchemaDDL)
    val schemaMappingExpr = schemaMapping(sourceSchema)
    stream.foreachRDD((rdd, _) => {
      val df = sparkSession
        .createDataFrame(
          rdd,
          StructType(Array(StructField("json", StringType)))
        )
        .select(from_json($"json", sourceSchema) as "data")
        .selectExpr(schemaMappingExpr: _*)
        streamingCallback(df)
    })
  }
}
