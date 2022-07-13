package com.github.sharpdata.sharpetl.spark.datasource.jdbc

import com.google.common.base.Strings.isNullOrEmpty
import com.github.sharpdata.sharpetl.core.util.Constants.DataSourceType.POSTGRES
import com.github.sharpdata.sharpetl.core.annotation._
import com.github.sharpdata.sharpetl.spark.utils.ETLSparkSession
import org.apache.spark.sql.types.{DataType, StringType, StructField}

@source(types = Array("postgres"))
@sink(types = Array("postgres"))
class PostgresDataSource extends AbstractJdbcDataSource(POSTGRES) {

  override def buildSelectSql(selectSql: String): String = s"($selectSql) as t"

  private val PRIMARY_KEY = "pri"

  override def getCols(targetDBName: String, targetTableName: String): (Seq[StructField], Seq[StructField]) = {
    var primaryCols = Seq[StructField]()
    var notPrimaryCols = Seq[StructField]()
    val (tableSchemaName, tableName) = if (targetTableName.contains('.')) {
      val splittedTableName = targetTableName.split("\\.")
      (splittedTableName(0), splittedTableName(1))
    } else {
      ("public", targetTableName)
    }
    this
      .load(
        ETLSparkSession.getHiveSparkSession(),
        Map(
          "dbtable" ->
            s"""
               |(
               |SELECT a.attnum
               |     , a.attname                                             AS field
               |     , pg_catalog.format_type(a.atttypid, a.atttypmod)       AS data_type
               |     , t.typname                                             AS type
               |     , a.attlen                                              AS length
               |     , a.atttypmod                                           AS lengthvar
               |     , a.attnotnull                                          AS notnull
               |     , b.description                                         AS comment
               |     , CASE WHEN pk.field is not null THEN 'PRI' ELSE '' END AS key
               |FROM pg_attribute a
               |         inner join pg_class c on a.attrelid = c.oid and a.attnum > 0
               |         LEFT JOIN pg_description b
               |                   ON a.attrelid = b.objoid
               |                       AND a.attnum = b.objsubid
               |         LEFT JOIN pg_constraint e
               |                   ON a.attrelid = e.conrelid
               |                       AND e.contype = 'p'
               |                       AND a.attnum = e.conkey[1]
               |         inner join pg_type t on a.atttypid = t.oid
               |         inner join pg_tables d on d.tablename = c.relname
               |         left join (SELECT pg_attribute.attname as field
               |                    FROM pg_index,
               |                         pg_class,
               |                         pg_attribute,
               |                         pg_namespace
               |                    WHERE pg_class.oid = '${appendSchema(tableSchemaName, processUpperCaseTableName(tableName))}'::regclass
               |                      AND indrelid = pg_class.oid
               |                      AND nspname = '$tableSchemaName'
               |                      AND pg_class.relnamespace = pg_namespace.oid
               |                      AND pg_attribute.attrelid = pg_class.oid
               |                      AND pg_attribute.attnum = any (pg_index.indkey)
               |                      AND indisprimary) pk on pk.field = a.attname
               |WHERE 1 = 1
               |  AND d.schemaname = '$tableSchemaName'
               |  AND c.relname = '${trimTableName(tableName)}'
               |  AND c.oid = '${appendSchema(tableSchemaName, processUpperCaseTableName(tableName))}'::regclass
               |ORDER BY a.attnum
               |) as t
               |""".stripMargin
        ),
        targetDBName
      )
      .collect()
      .foreach(row => {
        val field = row.getAs[String]("field")
        val dataType = row.getAs[String]("type")
        val key = row.getAs[String]("key")
        val sparkSQLDataType: DataType = dataTypeMapping.getOrElse(dataType, StringType)

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
    primaryCols ++ notPrimaryCols
  }

  override def makeUpsertSql(tableName: String,
                             primaryCols: Seq[StructField],
                             notPrimaryCols: Seq[StructField]): String = {
    s"""
       |insert into $tableName (${primaryCols.union(notPrimaryCols).map(_.name).mkString(", ")})
       |values (${primaryCols.union(notPrimaryCols).map(_ => "?").mkString(", ")})
       |on conflict (${primaryCols.map(_.name).mkString(", ")})
       |do update
       |set ${notPrimaryCols.map(_.name).map(col => s"$col = excluded.$col").mkString(", ")}
       |""".stripMargin
  }

  private def appendSchema(schema: String, table: String) = {
    if (isNullOrEmpty(schema)) {
      table
    } else {
      s"$schema.$table"
    }
  }

  private def processUpperCaseTableName(table: String): String = {
    if (table.startsWith("\"")) {
      table
    } else if (table.toLowerCase == table) {
      table
    } else {
      s""""$table""""
    }
  }

  private def quote(col: String): String = {
    if (col.startsWith("\"")) {
      col
    } else {
      s""""$col""""
    }
  }

  private def trimTableName(table: String): String = {
    if (table.startsWith("\"")) {
      table.replaceAll("\"", "")
    } else {
      table
    }
  }

  override def makeInsertSql(tableName: String,
                    primaryCols: Seq[StructField],
                    notPrimaryCols: Seq[StructField]): String = {
    s"""
       |INSERT INTO $tableName (${primaryCols.union(notPrimaryCols).map(it => quote(it.name)).mkString(", ")})
       |VALUES (${primaryCols.union(notPrimaryCols).map(_ => "?").mkString(", ")})
       |""".stripMargin
  }
}
