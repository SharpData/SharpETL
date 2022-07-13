package com.github.sharpdata.sharpetl.spark.test

import com.google.common.base.Strings.isNullOrEmpty
import com.github.sharpdata.sharpetl.core.datasource.config.BatchKafkaDataSourceConfig
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funspec.AnyFunSpec


class BatchKafkaMergeDFTest extends AnyFunSpec {

  lazy val spark: SparkSession = SparkSession
    .builder()
    .appName(this.getClass.getSimpleName).master("local")
    .getOrCreate()

  private val kafkaDataSourceConfig = new BatchKafkaDataSourceConfig()
  private val schemaDDL = "a String,b String,c String,d String,e String"
  private val sourceSchema: StructType = StructType.fromDDL(schemaDDL)
  private val schemaMappingExpr: Seq[String] = schemaMapping(sourceSchema)

  private val testDF: DataFrame = spark.createDataFrame(Seq(
    ("null", """{"a":"a1","b":"b1","c":"c1","d":"d1","e":"e1"}""", "bigDataAllRating.o", "2022-01-20 11:21:45"),
    ("null", """{"a":"a2","b":"b2","c":"c2","d":"d2","e":"e2"}""", "bigDataAllRating.o", "2022-01-21 11:21:45"),
    ("null", """{"a":"a3","b":"b3","c":"c3","d":"d3","e":"e3"}""", "bigDataAllRating.o", "2022-01-22 11:21:45")))
    .toDF("key", "value", "topic", "timestamp")

  private def schemaMapping: StructType => Seq[String] = {
    sourceSchema =>
      sourceSchema
        .fieldNames
        .map(fieldName => s"""data.$fieldName as $fieldName""")
  }

  private def getDF(columns:String): DataFrame = {
    import spark.implicits._

    val messageColumnNames = columns match {
      case value: String if !isNullOrEmpty(value) => kafkaDataSourceConfig.topicMessageColumns.split(",").map(_.trim)
      case _ => Array.empty[String]
    }

    val exprColumns = "CAST(value as STRING)" +: messageColumnNames.map(it => s"CAST($it as STRING)")
    val selectColumns = (from_json($"value", sourceSchema) as "data") +: messageColumnNames.map(col)
    val allSchemaMappingExpr = schemaMappingExpr ++ messageColumnNames.toSeq
    testDF.selectExpr(exprColumns: _*).select(selectColumns: _*).selectExpr(allSchemaMappingExpr: _*)
  }

  it("should get other columns") {
    kafkaDataSourceConfig.topicMessageColumns = "timestamp"
    val dataFrame = getDF(kafkaDataSourceConfig.topicMessageColumns)
    assert(dataFrame.columns.mkString(",") == "a,b,c,d,e,timestamp")
  }

  it("should not get other columns") {
    kafkaDataSourceConfig.topicMessageColumns = null
    val dataFrame = getDF(kafkaDataSourceConfig.topicMessageColumns)
    assert(dataFrame.columns.mkString(",") == "a,b,c,d,e")
  }
}
