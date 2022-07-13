package com.github.sharpdata.sharpetl.datasource.kafka

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{Encoder, Encoders}

import scala.jdk.CollectionConverters._

final case class OffsetRange(topic: String, partition: String, maxOffset: Long)

object OffsetRange {
  val offsetEncoder: Encoder[OffsetRange] = Encoders.product[OffsetRange]

  implicit class OffsetRangeConverter(values: List[OffsetRange]) {
    def asEndJson: String = {
      val offsetRangeObj = values.groupBy(_.topic)
        .map { case (topic, offsetRanges) =>
          (topic, offsetRanges.map(it => (it.partition, it.maxOffset)).toMap.asJava)
        }.asJava

      new ObjectMapper().writeValueAsString(offsetRangeObj)
    }
  }
}
