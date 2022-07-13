package com.github.sharpdata.sharpetl.spark.datasource

import com.github.sharpdata.sharpetl.core.api.Variables
import com.github.sharpdata.sharpetl.core.datasource.Source
import com.github.sharpdata.sharpetl.core.datasource.config.{DBDataSourceConfig, TextFileDataSourceConfig}
import com.github.sharpdata.sharpetl.core.repository.model.JobLog
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.core.util.CodecUtil
import com.github.sharpdata.sharpetl.core.annotation._
import com.github.sharpdata.sharpetl.datasource.hive.HiveMetaStoreUtil
import org.apache.spark.sql.types.StructType

import java.io.File

// scalastyle:off
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{FileInputFormat, JobConf, TextInputFormat}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
// scalastyle:on

@source(types = Array("text"))
class TextDataSource extends Source[DataFrame, SparkSession] {

  override def read(step: WorkflowStep, jobLog: JobLog, executionContext: SparkSession, variables: Variables): DataFrame = {
    load(executionContext, step, jobLog)
  }

  private def loadRdd(spark: SparkSession, sourceConfig: TextFileDataSourceConfig): RDD[String] = {
    val jobConf = new JobConf()

    val codec = CodecUtil.matchCodec(sourceConfig.getCodecExtension)
    if (codec.isDefined) {
      jobConf.set("io.compression.codecs", codec.get)
    }
    for (key <- sourceConfig.getOptions.keys) {
      jobConf.set(key, sourceConfig.getOptions()(key))
    }

    FileInputFormat.setInputPaths(jobConf, sourceConfig.getFilePath)
    spark.sparkContext
      .hadoopRDD(
        jobConf,
        classOf[TextInputFormat],
        classOf[LongWritable],
        classOf[Text]
      )
      .map { case (_, text) =>
        new String(text.getBytes, 0, text.getLength, sourceConfig.getEncoding())
      }
  }

  def load(
            spark: SparkSession,
            step: WorkflowStep,
            job: JobLog): DataFrame = {
    val sourceConfig = step.getSourceConfig[TextFileDataSourceConfig]
    val filePath: String = sourceConfig.getFilePath
    val rdd = loadRdd(spark, sourceConfig)
    val dataSourceConfig = step.getTargetConfig[DBDataSourceConfig]
    val targetTable: String = dataSourceConfig.getTableName
    val struct: StructType = HiveMetaStoreUtil.getHiveTableStructType(dataSourceConfig.dbName, targetTable)
    val columnsCount = struct.length - 3
    val fileName = filePath.substring(filePath.lastIndexOf(File.separator) + 1)
    val publicFields = Array(
      job.jobId,
      fileName,
      job.dataRangeEnd
    )
    val rowRDD = if (sourceConfig.getSeparator == null) {
      val fieldLengthConfig = sourceConfig.getFieldLengthConfig
      substrRowRDD(
        rdd,
        sourceConfig.getEncoding,
        fieldLengthConfig,
        sourceConfig.getStrictColumnNum.toBoolean,
        columnsCount,
        publicFields
      )
    } else {
      splitRowRDD(
        rdd,
        sourceConfig.getSeparator,
        columnsCount,
        sourceConfig.getStrictColumnNum.toBoolean,
        publicFields
      )
    }

    spark.createDataFrame(rowRDD, struct)
  }

  private def substrRowRDD(
                            rdd: RDD[String],
                            encoding: String,
                            fieldLengthConfig: String,
                            strictColumnNum: Boolean,
                            columnsCount: Int,
                            publicFields: Array[Any]): RDD[Row] = {
    val fieldLengthArray = fieldLengthConfig
      .split(",")
      .take(columnsCount)
      .map(_.toInt)
    val fieldStartEndIndexArray = fieldLengthArray.scanLeft(0)(_ + _)
    val lineLength = fieldStartEndIndexArray.last
    val filterFunction = if (strictColumnNum) {
      (s: String) => s.getBytes(encoding).length == lineLength
    } else {
      (s: String) => s.getBytes(encoding).length >= lineLength
    }
    rdd
      .filter(filterFunction)
      .map(str => {
        val arr = new Array[Any](fieldLengthArray.length)
        val bytes = str.getBytes(encoding)
        for (i <- arr.indices) {
          arr(i) = new String(
            java.util.Arrays.copyOfRange(bytes, fieldStartEndIndexArray(i), fieldStartEndIndexArray(i + 1)),
            encoding
          )
        }
        Row.fromSeq(
          Array.concat(
            arr,
            publicFields
          )
        )
      })
  }

  private def splitRowRDD(
                           rdd: RDD[String],
                           separator: String,
                           columnsCount: Int,
                           strictColumnNum: Boolean,
                           publicFields: Array[Any]): RDD[Row] = {
    rdd
      .filter(s => {
        if (strictColumnNum) {
          s.split(separator).length == columnsCount
        } else {
          s.split(separator).length >= columnsCount
        }
      })
      .map(str => {
        Row.fromSeq(
          Array.concat(
            str.split(separator).take(columnsCount).asInstanceOf[Array[Any]],
            publicFields
          )
        )
      })
  }

}
