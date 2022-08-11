package com.github.sharpdata.sharpetl.modeling.excel.model

object OdsTableConfigSheetHeader {
  val ODS_TABLE_CONFIG_SHEET_NAME = "ods_etl_config"

  val SOURCE_CONNECTION = "source_connection"
  val SOURCE_TABLE = "source_table"
  val SOURCE_DB = "source_db"
  val SOURCE_TYPE = "source_type"

  val TARGET_CONNECTION = "target_connection"
  val TARGET_TABLE = "target_table"
  val TARGET_DB = "target_db"
  val TARGET_TYPE = "target_type"

  val FILTER_EXPR = "row_filter_expression"

  val UPDATE_TYPE = "update_type"
  val PARTITION_FORMAT = "partition_format"
  val TIME_FORMAT = "time_format"
  val PERIOD = "period"
}

object OdsModelingSheetHeader {
  val ODS_MODELING_SHEET_NAME = "ods_config"

  val SOURCE_COLUMN = "source_column"
  val COLUMN_TYPE = "column_type"
  val INCREMENTAL_COLUMN = "incremental_column"
  val PRIMARY_COLUMN = "is_PK"

  val TARGET_COLUMN = "target_column"

  val EXTRA_COLUMN_EXPRESSION = "extra_column_expression"
}

object OdsTable {
  final case class OdsTableConfig(sourceConnection: String,
                                  sourceTable: String,
                                  sourceDb: String,
                                  sourceType: String,
                                  targetConnection: String,
                                  targetTable: String,
                                  targetDb: String,
                                  targetType: String,
                                  filterExpression: String,
                                  loadType: String,
                                  logDrivenType: String,
                                  upstream: String,
                                  dependsOn: String,
                                  defaultStart: String,
                                  partitionFormat: String,
                                  timeFormat: String,
                                  period: String
                                 )


  final case class OdsModelingColumn(sourceTable: String,
                                     targetTable: String,
                                     sourceColumn: String,
                                     targetColumn: String,
                                     extraColumnExpression: String,
                                     incrementalColumn: Boolean,
                                     primaryKeyColumn: Boolean)


  final case class OdsModeling(odsTableConfig: OdsTableConfig, columns: Seq[OdsModelingColumn])

}
