package com.github.sharpdata.sharpetl.modeling.formatConversion.model

object DbType {
  val ORACLE = "oracle"
  val SQL_SERVER = "sql_server"
  val MSSQL = "mssql"
  val HIVE = "hive"
  val CLICKHOUSE = "clickhouse"
  val POSTGRES = "postgres"
  val REDSHIFT = "redshift"
  val MYSQL = "mysql"
}

object OdsTablePartial {
  final case class ModelingColumn(source: String, target: String)
}

object TableConfigSheetHeader {
  val SOURCE = "source"
  val TARGET = "target"
}
