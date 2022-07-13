package com.github.sharpdata.sharpetl.spark.transformation

import com.github.sharpdata.sharpetl.modeling.excel.model.CreateDimMode
import com.google.gson.{Gson, JsonElement, JsonObject}
import com.github.sharpdata.sharpetl.core.util.Constants.DataSourceType.POSTGRES
import com.github.sharpdata.sharpetl.core.util.Constants.Separator.ENTER
import com.github.sharpdata.sharpetl.core.util.StringUtil.uuidName
import com.github.sharpdata.sharpetl.modeling.sql.dialect.SqlDialect.quote
import com.github.sharpdata.sharpetl.spark.datasource.HiveDataSource
import com.github.sharpdata.sharpetl.spark.utils.ETLSparkSession.sparkSession
import com.github.sharpdata.sharpetl.spark.datasource.connection.JdbcConnection
import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

import java.sql.Connection
import java.util.Map.Entry
import java.util.Properties
import scala.jdk.CollectionConverters._

// scalastyle:off
object JdbcAutoCreateDimTransformer extends Transformer {
  private def genStrListFromJson(jsonObj: JsonObject, separation: String): Seq[String] = {
    jsonObj.entrySet.asScala.map {
      entry: Entry[String, JsonElement] =>
        s"${entry.getKey}$separation${entry.getValue.getAsString}"
    }.toList
  }

  private def genKeyListFromJson(jsonObj: JsonObject): Seq[String] = {
    jsonObj.entrySet().asScala.map(_.getKey).toSet.toList.sorted
  }

  private def genValueListFromJson(jsonObj: JsonObject): Seq[String] = {
    jsonObj.entrySet.asScala.map {
      entry: Entry[String, JsonElement] =>
        entry.getValue.getAsString
    }.toList
  }

  private def loadTempDataToDb(conn: Connection,
                               dimTableColumnsAndTypeJsonObj: JsonObject,
                               currentAndDimColumnsMappingJsonObj: JsonObject,
                               currentAndDimPrimaryMappingJsonObj: JsonObject,
                               currentDbType: String,
                               currentDb: String,
                               updateTable: String,
                               businessCreateTime: String): String = {
    val conf = JdbcConnection(currentDb, currentDbType).getDefaultConfig
    val url = conf(JDBCOptions.JDBC_URL)
    val prop = new Properties()
    prop.setProperty(JDBCOptions.JDBC_DRIVER_CLASS, conf(JDBCOptions.JDBC_DRIVER_CLASS))
    prop.setProperty("user", conf("user"))
    prop.setProperty("password", conf("password"))

    val columns: Seq[String] = genStrListFromJson(dimTableColumnsAndTypeJsonObj, " ")

    val columnsStr = columns.toList.mkString(s", $ENTER")
    val tempTable = "temp_table_" + uuidName()
    val createSql =
      s"""
         |DROP TABLE IF EXISTS $tempTable;
         |CREATE TABLE ${tempTable}(
         |$columnsStr
         |);""".stripMargin

    conn.prepareStatement(createSql).execute()

    val selectColumns: Seq[String] = genStrListFromJson(currentAndDimColumnsMappingJsonObj, " as ")

    val dimColumns: Seq[String] = genValueListFromJson(currentAndDimColumnsMappingJsonObj)

    val currentPrimaryColumn: Seq[String] = genKeyListFromJson(currentAndDimPrimaryMappingJsonObj)

    val selectSql =
      s"""
         |select ${dimColumns.mkString(", ")}
         |from
         |(select ${selectColumns.mkString(", ")}, row_number() OVER (PARTITION BY ${currentPrimaryColumn.mkString(", ")} ORDER BY `$businessCreateTime` DESC) as row_number
         |from $updateTable) temp
         |where temp.row_number=1
         |""".stripMargin

    val df = new HiveDataSource().load(sparkSession, selectSql)
    df.write.mode(SaveMode.Append).jdbc(url, tempTable, prop)
    tempTable
  }

  private def loadInsertDataToDim(conn: Connection,
                                  currentTableName: String,
                                  primaryFields: Seq[String],
                                  tempTable: String,
                                  currentTableColumnsList: Seq[String],
                                  businessCreateTime: String): Unit = {
    val insertSql =
      s"""
         |with latest_current as (
         |    select * from $currentTableName
         |    where is_latest='1'
         |), insert_data as (
         |    select ${currentTableColumnsList.map(field => "tmp." + quote(field, POSTGRES)).mkString(", ")}
         |    from $tempTable tmp
         |    left join latest_current
         |    on ${primaryFields.map(field => "latest_current." + quote(field, POSTGRES) + " = tmp." + quote(field, POSTGRES)).mkString(" and ")}
         |    where latest_current.${quote(primaryFields.head, POSTGRES)} is null
         |)
         |insert into $currentTableName (${currentTableColumnsList.map(field => quote(field, POSTGRES)).mkString(", ")}, start_time, is_active, is_latest, is_auto_created)
         |select ${currentTableColumnsList.map(field => quote(field, POSTGRES)).mkString(", ")}, $businessCreateTime, '1', '1', '1' from insert_data;""".stripMargin
    conn.prepareStatement(insertSql).execute()
  }

  private def clearResource(tempTable: String, conn: Connection): Unit = {
    val dropSql =
      s"""
         |DROP TABLE IF EXISTS $tempTable;""".stripMargin
    conn.prepareStatement(dropSql).execute()
  }

  def loadData(args: collection.Map[String, String]): Unit = {
    val updateTable = args("updateTable")
    val createDimMode = args("createDimMode")

    val dimDb = args("dimDb")
    val dimDbType = args("dimDbType")
    val dimTable = args("dimTable")

    val currentBusinessCreateTime = args("currentBusinessCreateTime")
    val dimTableColumnsAndType = args("dimTableColumnsAndType")
    val currentAndDimColumnsMapping = args("currentAndDimColumnsMapping")
    val currentAndDimPrimaryMapping = args("currentAndDimPrimaryMapping")
    val conn = JdbcConnection(dimDb, dimDbType).getConnection()

    val dimTableColumnsAndTypeJsonObj: JsonObject = new Gson().fromJson(dimTableColumnsAndType, classOf[JsonObject])
    val currentAndDimColumnsMappingJsonObj: JsonObject = new Gson().fromJson(currentAndDimColumnsMapping, classOf[JsonObject])
    val currentAndDimPrimaryMappingJsonObj: JsonObject = new Gson().fromJson(currentAndDimPrimaryMapping, classOf[JsonObject])

    if (createDimMode.equals(CreateDimMode.ONCE)) {
      val tempTable = loadTempDataToDb(conn, dimTableColumnsAndTypeJsonObj, currentAndDimColumnsMappingJsonObj,
        currentAndDimPrimaryMappingJsonObj, dimDbType, dimDb, updateTable, currentBusinessCreateTime)

      val dimPrimaryFields: Seq[String] = genValueListFromJson(currentAndDimPrimaryMappingJsonObj)

      val dimTableColumnsList: Seq[String] = genValueListFromJson(currentAndDimColumnsMappingJsonObj)

      loadInsertDataToDim(conn, dimTable, dimPrimaryFields, tempTable, dimTableColumnsList, currentAndDimColumnsMappingJsonObj.getAsJsonPrimitive(currentBusinessCreateTime).getAsString)
      clearResource(tempTable, conn)
      conn.close()
    } else if (createDimMode.equals(CreateDimMode.ALWAYS)) {
    } else {

    }
  }

  override def transform(args: Map[String, String]): DataFrame = {
    loadData(args)
    sparkSession.emptyDataFrame
  }
}
// scalastyle:on