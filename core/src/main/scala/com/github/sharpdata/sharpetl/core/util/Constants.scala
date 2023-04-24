package com.github.sharpdata.sharpetl.core.util

import java.time.LocalDateTime
import scala.util.matching.Regex

object Constants {
  object Job {
    // We should not use null in scala, but mybatis could not process Option type.
    // So I centralized the null only in this place
    val nullDataTime: LocalDateTime = null // scalastyle:ignore
  }

  object Environment {
    var CURRENT: String = _
    val LOCAL = "local"
    val DEV = "dev"
    val QA = "qa"
    val PROD = "prod"
    val TEST = "test"
    val EMBEDDED_HIVE = "embedded-hive"
  }

  object PathPrefix {
    val FILE = ""
    val HDFS = "hdfs"
    val DBFS = "dbfs"
    val OSS = "oss"
  }

  object Encoding {
    val UTF8 = "UTF-8"
    val GBK = "GBK"
    val ISO_8859_1 = "ISO-8859-1"
  }

  object Separator {
    val COMMA = ","
    val ENTER = "\n"
  }

  object BooleanString {
    val TRUE = true.toString
    val FALSE = false.toString
  }

  object ETLDatabaseType {
    val MYSQL = "mysql"
    val MSSQL = "mssql"
    val H2 = "h2"
    val SPARK_SHARP_ETL = "spark_sharp_etl"
  }

  object DataSourceType extends Serializable {
    val TRANSFORMATION = "transformation"
    val CSV: String = "csv"
    val CONSOLE: String = "console"
    val VARIABLES: String = "variables"
    val HIVE: String = "hive"
    val SPARK_SQL: String = "spark_sql"
    val ORACLE: String = "oracle"
    val MYSQL: String = "mysql"
    val MS_SQL_SERVER: String = "ms_sql_server"
    val POSTGRES: String = "postgres"
    val H2: String = "h2"
    val TEMP: String = "temp"
    val ES: String = "es"
    // 直接在 kudu 中建的 kudu 表，直接使用 kudu 中的表名
    val KUDU: String = "kudu"
    // 在 impala 中建的 kudu 表，使用 impala 中的表名（与 kudu 中实际的表名不同）
    val IMPALA_KUDU: String = "impala_kudu"
    val IMPALA: String = "impala"
    val MOUNT: String = "mount"
    val FTP: String = "ftp"
    val HDFS: String = "hdfs"
    val DELTA_LAKE: String = "delta_lake"
    val SCP: String = "scp"
    val SFTP: String = "sftp"
    val JSON: String = "json"
    val EXCEL: String = "excel"
    val BATCH_KAFKA: String = "batch_kafka"
    val STREAMING_KAFKA: String = "streaming_kafka"
    // object 和 class 在注册 udf 时需要配置
    // 大多数情况使用 object 即可，使用 class 的场景参考 com.github.sharpdata.sharpetl.spark.udf.PmmlUDF
    val OBJECT = "object"
    val CLASS = "class"
    // pmml 模型加载并注册 udf（特殊的 udf）
    val PMML = "pmml"
    val UDF = "udf"
    val DO_NOTHING = "do_nothing"
    val INFORMIX = "informix"
    val COMPRESSTAR = "compresstar"
    val BIGQUERY: String = "bigquery"
    val TEXT = "text"
  }

  object WriteMode {
    val OVER_WRITE: String = "overwrite"
    val APPEND: String = "append"
    val UPSERT: String = "upsert"
    val DELETE: String = "delete"
    val EXECUTE: String = "execute"
    val MERGE_WRITE: String = "mergewrite"
  }

  object IncrementalType {
    val DIFF: String = "diff"
    val AUTO_INC_ID: String = "auto_inc_id"
    val KAFKA_OFFSET: String = "kafka_offset"
    val UPSTREAM: String = "upstream"
    val TIMEWINDOW: String = "timewindow"
  }

  object IO_COMPRESSION_CODEC_CLASS {
    val GZ_CODEC_CLASS: String = "org.apache.hadoop.io.compress.GzipCodec"
    val GZC_CODEC_CLASS: String = "com.github.sharpdata.sharpetl.spark.extension.CGzipCodecExtension"
    val DEFAULT_CODEC_CLASS: String = "org.apache.hadoop.io.compress.DefaultCodec"

    val IO_COMPRESSION_CODEC_CLASS_NAMES: String = String.join(
      ",",
      GZ_CODEC_CLASS,
      GZC_CODEC_CLASS,
      DEFAULT_CODEC_CLASS
    )
  }

  /**
   * 解析resoucese/task中sql文件的正则表达式
   */
  object Pattern {
    val REPARTITION_NUM_PATTERN: Regex = """^[1-9][0-9]*$""".r
    val REPARTITION_COLUMNS_PATTERN: Regex = """^[a-zA-Z_][0-9a-zA-Z_]*(,[a-zA-Z_][0-9a-zA-Z_]*)*$""".r
    val REPARTITION_NUM_COLUMNS_PATTERN: Regex = """^[1-9][0-9]*(,[a-zA-Z_][0-9a-zA-Z_]*)+$""".r
  }

  object JdbcDataType {
    val VARCHAR = "varchar"
    val VARCHAR2 = "varchar2"
    val BPCHAR = "bpchar"
    val TEXT = "text"
    val JSONB = "jsonb"
    val CHAR = "char"

    val TIMESTAMP = "timestamp"
    val TIMESTAMPTZ = "timestamptz"
    val DATE = "date"
    val DATETIME = "datetime"

    val NUMERIC = "numeric"
    val NUMBER = "number"

    val INT4 = "int4"
    val INT8 = "int8"
    val BIGINT = "bigint"
    val INT = "int"
    val BIT = "bit"
    val TINYINT = "tinyint"
    val DOUBLE = "double"
    val ROWID = "rowid"
    val DECIMAL = "decimal"
  }

  object LoadType {
    val FULL = "full"
    val INCREMENTAL = "incremental"
  }

  object TransformerType {
    val OBJECT_TYPE = "object"
    val CLASS_TYPE = "class"
    val DYNAMIC_OBJECT_TYPE = "dynamic_object"
  }

  object PeriodType {
    val DAY = 1440
    val HOUR = 60
  }

}
