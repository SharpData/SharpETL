package com.github.sharpdata.sharpetl.modeling.excel.model

import com.google.common.base.Strings.isNullOrEmpty
import com.github.sharpdata.sharpetl.modeling.sql.gen.DwdExtractSqlGen.ZIP_ID_FLAG

object DwdTableConfigSheetHeader {
  val DWD_TABLE_CONFIG_SHEET_NAME = "dwd_etl_config"

  val SOURCE_CONNECTION = "source_connection"
  val SOURCE_TYPE = "source_type"
  val SOURCE_DB = "source_db"
  val SOURCE_TABLE = "source_table"
  val TARGET_CONNECTION = "target_connection"
  val TARGET_TYPE = "target_type"
  val TARGET_DB = "target_db"
  val TARGET_TABLE = "target_table"
  val FACT_OR_DIM = "fact_or_dim"
  val SLOW_CHANGING = "slow_changing"
  val ROW_FILTER_EXPRESSION = "row_filter_expression"
  val UPDATE_TYPE = "update_type"
}

object DwdModelingSheetHeader {
  val DWD_MODELING_SHEET_NAME = "dwd_config" // TODO: rename

  val SOURCE_TABLE = "source_table"
  val TARGET_TABLE = "target_table"
  val SOURCE_COLUMN = "source_column"
  val SOURCE_COLUMN_DESCRIPTION = "source_column_description"
  val TARGET_COLUMN = "target_column"
  val TARGET_COLUMN_TYPE = "target_column_type"
  val EXTRA_COLUMN_EXPRESSION = "extra_column_expression"
  val PARTITION_COLUMN = "partition_column"
  val LOGIC_PRIMARY_COLUMN = "logic_primary_column"
  val JOIN_DB_CONNECTION = "join_db_connection"
  val JOIN_DB_TYPE = "join_db_type"
  val JOIN_DB = "join_db"
  val JOIN_TABLE = "join_table"
  val JOIN_ON = "join_on"
  val CREATE_DIM_MODE = "create_dim_mode"
  val JOIN_TABLE_COLUMN = "join_table_column"
  val BUSINESS_CREATE_TIME = "business_create_time"
  val BUSINESS_UPDATE_TIME = "business_update_time"
  val IGNORE_CHANGING_COLUMN = "ignore_changing_column"
  val QUALITY_CHECK_RULES = "quality_check_rules"
}

object FactOrDim {
  val FACT = "fact"
  val DIM = "dim"
}

object CreateDimMode {
  val NEVER = "never"
  val ONCE = "once"
  val ALWAYS = "always"
}

/**
 * 粒度: sourceTable + targetTable
 * TODO 暂未考虑质量check
 */
final case class DwdTableConfig(sourceConnection: String,
                                sourceType: String,
                                sourceDb: String,
                                sourceTable: String,
                                targetConnection: String,
                                targetType: String,
                                targetDb: String,
                                targetTable: String,
                                factOrDim: String,
                                slowChanging: Boolean,
                                rowFilterExpression: String,
                                updateType: String)


final case class DwdModelingColumn(sourceTable: String,
                                   targetTable: String,
                                   sourceColumn: String,
                                   sourceColumnDescription: String,
                                   targetColumn: String,
                                   targetColumnType: String,
                                   extraColumnExpression: String,
                                   partitionColumn: Boolean,
                                   logicPrimaryColumn: Boolean,
                                   joinDbConnection: String,
                                   joinDbType: String,
                                   joinDb: String,
                                   joinTable: String,
                                   joinOn: String,
                                   createDimMode: String,
                                   joinTableColumn: String,
                                   businessCreateTime: Boolean,
                                   businessUpdateTime: Boolean,
                                   ignoreChangingColumn: Boolean,
                                   qualityCheckRules: String) {
  lazy val joinTempFieldPrefix = s"${joinDb}_${joinTable}____"
}


final case class DwdModeling(dwdTableConfig: DwdTableConfig, columns: Seq[DwdModelingColumn])

final case class DimTable(dimTable: String, cols: Seq[DwdModelingColumn], partitionCols: Seq[DwdModelingColumn],
                          updateTimeCols: Seq[DwdModelingColumn], createTimeCols: Seq[DwdModelingColumn]) {
  val noneAutoCreateDimIdColumns: Seq[DwdModelingColumn] = cols.filterNot(_.extraColumnExpression == ZIP_ID_FLAG)

  val joinOnColumns: Seq[DwdModelingColumn] = cols.filterNot(it => isNullOrEmpty(it.joinOn))

  val autoCreateColumns: Seq[DwdModelingColumn] = cols.filter(_.extraColumnExpression == ZIP_ID_FLAG)

  val additionalCols: Seq[DwdModelingColumn] = partitionCols ++ updateTimeCols ++ createTimeCols
}
