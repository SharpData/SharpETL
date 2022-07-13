package com.github.sharpdata.sharpetl.spark.datasource.jdbc

import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

import com.github.sharpdata.sharpetl.core.util.Constants.DataSourceType.POSTGRES


class MockPostgresDataSource extends AbstractJdbcDataSource(POSTGRES) {
  override def getCols(
      targetDBName: String,
      targetTableName: String
  ): (Seq[StructField], Seq[StructField]) = (
    Seq(StructField("id", StringType)),
    Seq(StructField("code", StringType))
  )

  override def makeUpsertCols(
      primaryCols: Seq[StructField],
      notPrimaryCols: Seq[StructField]
  ): Seq[StructField] = {
    primaryCols ++ notPrimaryCols
  }

  override def makeUpsertSql(
      tableName: String,
      primaryCols: Seq[StructField],
      notPrimaryCols: Seq[StructField]
  ): String = ""
}
