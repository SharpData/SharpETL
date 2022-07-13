package com.github.sharpdata.sharpetl.spark.datasource.jdbc

import com.github.sharpdata.sharpetl.core.exception.Exception.IncompleteDataSourceException
import com.github.sharpdata.sharpetl.core.util.Constants.DataSourceType.MYSQL
import com.github.sharpdata.sharpetl.core.util.StringUtil
import com.github.sharpdata.sharpetl.core.annotation._
import com.github.sharpdata.sharpetl.spark.utils.ETLSparkSession
import org.apache.spark.sql.types._

@source(types = Array("mysql", "h2"))
@sink(types = Array("mysql", "h2"))
class MysqlDataSource extends AbstractJdbcDataSource(MYSQL) {

  override def buildSelectSql(selectSql: String): String = s"($selectSql) as t"

  private val PRIMARY_KEY = "pri"

  override def getCols(targetDBName: String, targetTableName: String): (Seq[StructField], Seq[StructField]) = {
    var primaryCols = Seq[StructField]()
    var notPrimaryCols = Seq[StructField]()
    if (StringUtil.isNullOrEmpty(targetDBName)) {
      throw IncompleteDataSourceException("MySQL target database name is empty!")
    }
    if (StringUtil.isNullOrEmpty(targetTableName)) {
      throw IncompleteDataSourceException("MySQL target table name is empty!")
    }
    this
      .load(
        ETLSparkSession.getHiveSparkSession(),
        Map(
          "dbtable" ->
            s"""
               |(
               |    SELECT a.table_schema as table_schema,
               |           a.table_name   as table_name,
               |           b.column_name  as column_name,
               |           b.column_type  as column_type,
               |           b.column_key   as column_key
               |    FROM information_schema.TABLES a
               |             LEFT JOIN information_schema.COLUMNS b ON a.table_name = b.TABLE_NAME AND
               |                                                       a.TABLE_SCHEMA = b.TABLE_SCHEMA
               |    WHERE a.TABLE_SCHEMA = '$targetDBName'
               |      and a.table_name = '$targetTableName'
               |    ORDER BY a.table_name, b.ORDINAL_POSITION
               |) as t
               |""".stripMargin
        ),
        targetDBName
      )
      .collect()
      .foreach(row => {
        val field = row.getAs[String]("column_name")
        val dataType = row.getAs[String]("column_type")
        val key = row.getAs[String]("column_key")
        val dataTypePattern = "(\\S*)\\(\\S*\\)".r

        val jdbcDataType: String = dataTypePattern.findFirstMatchIn(dataType.trim.toLowerCase) match {
          case Some(data) => data group 1
          case _ => ""
        }

        val sparkSQLDataType: DataType = dataTypeMapping.getOrElse(jdbcDataType, StringType)

        if (key.toLowerCase() == PRIMARY_KEY) {
          primaryCols :+= StructField(field, sparkSQLDataType)
        } else {
          notPrimaryCols :+= StructField(field, sparkSQLDataType)
        }
      })
    (primaryCols, notPrimaryCols)
  }

  override def makeUpsertCols(primaryCols: Seq[StructField],
                              notPrimaryCols: Seq[StructField]): Seq[StructField] = {
    primaryCols ++ notPrimaryCols ++ notPrimaryCols
  }

  override def makeUpsertSql(tableName: String,
                             primaryCols: Seq[StructField],
                             notPrimaryCols: Seq[StructField]): String = {
    s"""
       |INSERT INTO ${tableName} (${primaryCols.union(notPrimaryCols).map(_.name).mkString(", ")})
       |VALUES (${primaryCols.union(notPrimaryCols).map(_ => "?").mkString(", ")})
       |ON DUPLICATE KEY UPDATE ${notPrimaryCols.map(_.name).map(col => s"$col = ?").mkString(", ")}
       |""".stripMargin
  }

}
