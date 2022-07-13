package com.github.sharpdata.sharpetl.spark.transformation

import com.google.gson.{Gson, JsonElement, JsonObject}
import com.github.sharpdata.sharpetl.core.util.Constants.LoadType.INCREMENTAL
import com.github.sharpdata.sharpetl.core.util.Constants.DataSourceType.POSTGRES
import com.github.sharpdata.sharpetl.core.util.Constants.Separator.ENTER
import com.github.sharpdata.sharpetl.core.util.DateUtil.YYYY_MM_DD_HH_MM_SS
import com.github.sharpdata.sharpetl.core.util.ETLLogger
import com.github.sharpdata.sharpetl.core.util.StringUtil.uuidName
import com.github.sharpdata.sharpetl.modeling.sql.dialect.SqlDialect.quote
import com.github.sharpdata.sharpetl.spark.datasource.HiveDataSource
import com.github.sharpdata.sharpetl.spark.utils.ETLSparkSession.sparkSession
import com.github.sharpdata.sharpetl.spark.datasource.connection.JdbcConnection
import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

import java.sql.Connection
import java.util.Map.Entry
import java.util.{Date, Properties}
import scala.jdk.CollectionConverters._

// scalastyle:off

object JdbcLoadTransformer extends Transformer {
  private def loadTempDataToDb(conn: Connection,
                               currentTableColumnsAndType: JsonObject,
                               currentDbType: String,
                               currentDb: String,
                               updateTable: String): String = {
    val conf = JdbcConnection(currentDb, currentDbType).getDefaultConfig
    val url = conf(JDBCOptions.JDBC_URL)
    val prop = new Properties()
    prop.setProperty(JDBCOptions.JDBC_DRIVER_CLASS, conf(JDBCOptions.JDBC_DRIVER_CLASS))
    prop.setProperty("user", conf("user"))
    prop.setProperty("password", conf("password"))

    val columns: Seq[String] =
      currentTableColumnsAndType.entrySet.asScala.map {
        entry: Entry[String, JsonElement] =>
          s"${quote(entry.getKey, currentDbType)} ${entry.getValue.getAsString}"
      }.toList

    val columnsStr = columns.toList.mkString(s", $ENTER")
    val tempTable = "temp_table_" + uuidName()
    val createSql =
      s"""
         |DROP TABLE IF EXISTS $tempTable;
         |CREATE TABLE ${tempTable}(
         |$columnsStr
         |);""".stripMargin

    ETLLogger.info(s"Executing SQL: \n $createSql")

    conn.prepareStatement(createSql).execute()

    val selectSql =
      s"""
         |select * from $updateTable
         |""".stripMargin

    val df = new HiveDataSource().load(sparkSession, selectSql)
    df.write.mode(SaveMode.Append).jdbc(url, tempTable, prop)
    tempTable
  }

  private def loadDeleteTempDataToDb(conn: Connection,
                                     currentTableName: String,
                                     primaryFields: Seq[String],
                                     tempTable: String,
                                     currentTableColumnsList: Seq[String],
                                     businessUpdateTime: String): String = {
    val nowDateTime = YYYY_MM_DD_HH_MM_SS.format(new Date())
    val tempDeleteTable = "temp_delete_table_" + uuidName()
    val createDeleteTempSql =
      s"""
         |create temp table $tempDeleteTable as
         |with latest_current as (
         |    select * from $currentTableName
         |    where is_latest='1'
         |), delete_data as (
         |    select ${currentTableColumnsList.filterNot(_.equals(businessUpdateTime)).map(field => "latest_current." + quote(field, POSTGRES)).mkString(", ")}, to_timestamp('$nowDateTime', 'yyyy-MM-dd HH24:mi:ss') as $businessUpdateTime, '0' as is_active
         |    from latest_current
         |    left join $tempTable tmp
         |    on ${primaryFields.map(field => "latest_current." + quote(field, POSTGRES) + " = tmp." + quote(field, POSTGRES)).mkString(" and ")}
         |    where tmp.${quote(primaryFields.head, POSTGRES)} is null
         |), delete_data_for_load as (
         |    select ${currentTableColumnsList.map(field => "delete_data." + quote(field, POSTGRES)).mkString(", ")}, delete_data.is_active
         |    from delete_data
         |    inner join latest_current
         |    on ${primaryFields.map(field => "delete_data." + quote(field, POSTGRES) + " = latest_current." + quote(field, POSTGRES)).mkString(" and ")}  and latest_current.is_active != delete_data.is_active
         |)
         |select * from delete_data_for_load;""".stripMargin
    conn.prepareStatement(createDeleteTempSql).execute()
    tempDeleteTable
  }

  private def loadUpdateTempDataToDb(conn: Connection,
                                     currentTableName: String,
                                     primaryFields: Seq[String],
                                     tempTable: String,
                                     currentTableColumnsList: Seq[String],
                                     businessUpdateTime: String): String = {
    val tempUpdateTable = "temp_update_table_" + uuidName()
    val createUpdateTempSql =
      s"""
         |create temp table $tempUpdateTable as
         |with latest_current as (
         |    select * from $currentTableName
         |    where is_latest='1'
         |), update_data as (
         |    select ${currentTableColumnsList.map(field => "tmp." + quote(field, POSTGRES)).mkString(", ")}, '1' as is_active
         |    from latest_current
         |    inner join $tempTable tmp
         |    on ${primaryFields.map(field => "latest_current." + quote(field, POSTGRES) + " = tmp." + quote(field, POSTGRES)).mkString(" and ")}
         |    and (${currentTableColumnsList.diff(primaryFields).filterNot(_.equals(businessUpdateTime)).map(field => "latest_current." + quote(field, POSTGRES) + " != tmp." + quote(field, POSTGRES)).mkString(" or ")})
         |    and latest_current.${quote(businessUpdateTime, POSTGRES)} <= tmp.${quote(businessUpdateTime, POSTGRES)}
         |)
         |select * from update_data;""".stripMargin
    conn.prepareStatement(createUpdateTempSql).execute()
    tempUpdateTable
  }

  private def loadInsertTempDataToDb(conn: Connection,
                                     currentTableName: String,
                                     primaryFields: Seq[String],
                                     tempTable: String,
                                     currentTableColumnsList: Seq[String]): String = {
    val tempInsertTable = "temp_insert_table_" + uuidName()
    val createInsertTempSql =
      s"""
         |create temp table $tempInsertTable as
         |with latest_current as (
         |    select * from $currentTableName
         |    where is_latest='1'
         |), insert_data as (
         |    select ${currentTableColumnsList.map(field => "tmp." + quote(field, POSTGRES)).mkString(", ")}, '1' as is_active
         |    from $tempTable tmp
         |    left join latest_current
         |    on ${primaryFields.map(field => "latest_current." + quote(field, POSTGRES) + " = tmp." + quote(field, POSTGRES)).mkString(" and ")}
         |    where latest_current.${quote(primaryFields.head, POSTGRES)} is null
         |)
         |select * from insert_data;""".stripMargin
    conn.prepareStatement(createInsertTempSql).execute()
    tempInsertTable
  }

  private def execSCFull(conn: Connection,
                         currentTableName: String,
                         primaryFields: Seq[String],
                         tempTable: String,
                         currentTableColumnsList: Seq[String],
                         businessCreateTime: String,
                         businessUpdateTime: String): Unit = {
    val tempDeleteTable = loadDeleteTempDataToDb(conn, currentTableName, primaryFields, tempTable, currentTableColumnsList, businessUpdateTime)
    val tempUpdateTable = loadUpdateTempDataToDb(conn, currentTableName, primaryFields, tempTable, currentTableColumnsList, businessUpdateTime)
    val tempInsertTable = loadInsertTempDataToDb(conn, currentTableName, primaryFields, tempTable, currentTableColumnsList)

    val updateSql =
      s"""
         |with update_data_for_load as (
         |    select ${primaryFields.map(field => quote(field, POSTGRES)).mkString(", ")}, $businessUpdateTime
         |    from $tempDeleteTable
         |    union all
         |    select ${primaryFields.map(field => quote(field, POSTGRES)).mkString(", ")}, $businessUpdateTime
         |    from $tempUpdateTable
         |)
         |update $currentTableName
         |set end_time=update_data_for_load.$businessUpdateTime, is_latest='0'
         |from update_data_for_load
         |where ${primaryFields.map(field => currentTableName + "." + quote(field, POSTGRES) + " = update_data_for_load." + quote(field, POSTGRES)).mkString(" and ")} and $currentTableName.is_latest='1'""".stripMargin

    val insertSql =
      s"""
         |with insert_data_for_load as (
         |    select ${currentTableColumnsList.map(field => quote(field, POSTGRES)).mkString(", ")}, $businessUpdateTime as start_time, is_active, '1' as is_latest from $tempDeleteTable
         |    union all
         |    select ${currentTableColumnsList.map(field => quote(field, POSTGRES)).mkString(", ")}, $businessUpdateTime as start_time, is_active, '1' as is_latest from $tempUpdateTable
         |    union all
         |    select ${currentTableColumnsList.map(field => quote(field, POSTGRES)).mkString(", ")}, $businessCreateTime as start_time, is_active, '1' as is_latest from $tempInsertTable
         |)
         |insert into $currentTableName (${currentTableColumnsList.map(field => quote(field, POSTGRES)).mkString(", ")}, start_time, is_active, is_latest)
         |select ${currentTableColumnsList.map(field => quote(field, POSTGRES)).mkString(", ")}, start_time, is_active, is_latest from insert_data_for_load;""".stripMargin
    conn.prepareStatement(updateSql).execute()
    conn.prepareStatement(insertSql).execute()
  }

  private def execSCIncremental(conn: Connection,
                                currentTableName: String,
                                primaryFields: Seq[String],
                                tempTable: String,
                                currentTableColumnsList: Seq[String],
                                businessCreateTime: String,
                                businessUpdateTime: String): Unit = {

    val tempUpdateTable = loadUpdateTempDataToDb(conn, currentTableName, primaryFields, tempTable, currentTableColumnsList, businessUpdateTime)
    val tempInsertTable = loadInsertTempDataToDb(conn, currentTableName, primaryFields, tempTable, currentTableColumnsList)

    val updateSql =
      s"""
         |with update_data_for_load as (
         |    select ${primaryFields.map(field => quote(field, POSTGRES)).mkString(", ")}, $businessUpdateTime
         |    from $tempUpdateTable
         |)
         |update $currentTableName
         |set end_time=update_data_for_load.$businessUpdateTime, is_latest='0'
         |from update_data_for_load
         |where ${primaryFields.map(field => currentTableName + "." + quote(field, POSTGRES) + " = update_data_for_load." + quote(field, POSTGRES)).mkString(" and ")} and $currentTableName.is_latest='1'""".stripMargin

    val insertSql =
      s"""
         |with insert_data_for_load as (
         |    select ${currentTableColumnsList.map(field => quote(field, POSTGRES)).mkString(", ")}, $businessUpdateTime as start_time, is_active, '1' as is_latest from $tempUpdateTable
         |    union all
         |    select ${currentTableColumnsList.map(field => quote(field, POSTGRES)).mkString(", ")}, $businessCreateTime as start_time, is_active, '1' as is_latest from $tempInsertTable
         |)
         |insert into $currentTableName (${currentTableColumnsList.map(field => quote(field, POSTGRES)).mkString(", ")}, start_time, is_active, is_latest)
         |select ${currentTableColumnsList.map(field => quote(field, POSTGRES)).mkString(", ")}, start_time, is_active, is_latest from insert_data_for_load;""".stripMargin
    conn.prepareStatement(updateSql).execute()
    conn.prepareStatement(insertSql).execute()
  }

  private def execNoSCFull(conn: Connection,
                           currentTableName: String,
                           primaryFields: Seq[String],
                           tempTable: String,
                           currentTableColumnsList: Seq[String],
                           businessUpdateTime: String): Unit = {
    val deleteSql =
      s"""
         |with latest_current as (
         |    select *
         |    from $currentTableName
         |), delete_data as (
         |    select ${primaryFields.map(field => "latest_current." + quote(field, POSTGRES)).mkString(", ")}
         |    from latest_current
         |    left join ${quote(tempTable, POSTGRES)} tmp
         |    on ${primaryFields.map(field => "latest_current." + quote(field, POSTGRES) + " = tmp." + quote(field, POSTGRES)).mkString(" and ")}
         |    where tmp.${quote(primaryFields.head, POSTGRES)} is null
         |), update_data as (
         |    select ${primaryFields.map(field => "tmp." + quote(field, POSTGRES)).mkString(", ")}
         |    from latest_current
         |--  此处的or语句中，需要把update_time过滤掉
         |    inner join ${quote(tempTable, POSTGRES)} tmp
         |    on ${primaryFields.map(field => "latest_current." + quote(field, POSTGRES) + " = tmp." + quote(field, POSTGRES)).mkString(" and ")}
         |    and (${currentTableColumnsList.diff(primaryFields).filterNot(_.equals(businessUpdateTime)).map(field => "latest_current." + quote(field, POSTGRES) + " != tmp." + quote(field, POSTGRES)).mkString(" or ")})
         |    and latest_current.${quote(businessUpdateTime, POSTGRES)} <= tmp.${quote(businessUpdateTime, POSTGRES)}
         |), delete_data_for_load as (
         |    select ${primaryFields.map(quote(_, POSTGRES)).mkString(", ")} from delete_data
         |    union all
         |    select ${primaryFields.map(quote(_, POSTGRES)).mkString(", ")} from update_data
         |)
         |delete from $currentTableName
         |using delete_data_for_load
         |where ${primaryFields.map(field => currentTableName + "." + quote(field, POSTGRES) + " = delete_data_for_load." + quote(field, POSTGRES)).mkString(" and ")};""".stripMargin

    val insertSql =
      s"""
         |with latest_current as (
         |    select *
         |    from $currentTableName
         |), insert_data as (
         |     select ${currentTableColumnsList.map("tmp." + quote(_, POSTGRES)).mkString(", ")}
         |     from ${quote(tempTable, POSTGRES)} tmp
         |     left join latest_current
         |     on ${primaryFields.map(field => "latest_current." + quote(field, POSTGRES) + " = tmp." + quote(field, POSTGRES)).mkString(" and ")}
         |     where latest_current.${quote(primaryFields.head, POSTGRES)} is null
         |), update_data as (
         |    select ${currentTableColumnsList.map("tmp." + quote(_, POSTGRES)).mkString(", ")}
         |    from latest_current
         |--  此处的or语句中，需要把update_time过滤掉
         |    inner join ${quote(tempTable, POSTGRES)} tmp
         |    on ${primaryFields.map(field => "latest_current." + quote(field, POSTGRES) + " = tmp." + quote(field, POSTGRES)).mkString(" and ")}
         |    and (${currentTableColumnsList.diff(primaryFields).filterNot(_.equals(businessUpdateTime)).map(field => "latest_current." + quote(field, POSTGRES) + " != tmp." + quote(field, POSTGRES)).mkString(" or ")})
         |    and latest_current.${quote(businessUpdateTime, POSTGRES)} <= tmp.${quote(businessUpdateTime, POSTGRES)}
         |), insert_data_for_load as (
         |    select ${currentTableColumnsList.map(quote(_, POSTGRES)).mkString(", ")} from update_data
         |    union all
         |    select ${currentTableColumnsList.map(quote(_, POSTGRES)).mkString(", ")} from insert_data
         |)
         |insert into $currentTableName (${currentTableColumnsList.map(quote(_, POSTGRES)).mkString(", ")})
         |select ${currentTableColumnsList.map(quote(_, POSTGRES)).mkString(", ")} from insert_data_for_load;""".stripMargin
    conn.prepareStatement(deleteSql).execute()
    conn.prepareStatement(insertSql).execute()
  }

  private def execNoSCIncremental(conn: Connection,
                                  currentTableName: String,
                                  primaryFields: Seq[String],
                                  tempTable: String,
                                  currentTableColumnsList: Seq[String],
                                  businessUpdateTime: String): Unit = {
    val deleteSql =
      s"""
         |with latest_current as (
         |    select *
         |    from $currentTableName
         |), update_data as (
         |    select ${primaryFields.map(field => "tmp." + quote(field, POSTGRES)).mkString(", ")}
         |    from latest_current
         |--  此处的or语句中，需要把update_time过滤掉
         |    inner join ${quote(tempTable, POSTGRES)} tmp
         |    on ${primaryFields.map(field => "latest_current." + quote(field, POSTGRES) + " = tmp." + quote(field, POSTGRES)).mkString(" and ")}
         |    and (${currentTableColumnsList.diff(primaryFields).filterNot(_.equals(businessUpdateTime)).map(field => "latest_current." + quote(field, POSTGRES) + " != tmp." + quote(field, POSTGRES)).mkString(" or ")})
         |    and latest_current.${quote(businessUpdateTime, POSTGRES)} <= tmp.${quote(businessUpdateTime, POSTGRES)}
         |), delete_data_for_load as (
         |    select ${primaryFields.map(quote(_, POSTGRES)).mkString(", ")} from update_data
         |)
         |delete from $currentTableName
         |using delete_data_for_load
         |where ${primaryFields.map(field => currentTableName + "." + quote(field, POSTGRES) + " = delete_data_for_load." + quote(field, POSTGRES)).mkString(" and ")};""".stripMargin

    val insertSql =
      s"""
         |with latest_current as (
         |    select *
         |    from $currentTableName
         |), insert_data as (
         |     select ${currentTableColumnsList.map("tmp." + quote(_, POSTGRES)).mkString(", ")}
         |     from ${quote(tempTable, POSTGRES)} tmp
         |     left join latest_current
         |     on ${primaryFields.map(field => "latest_current." + quote(field, POSTGRES) + " = tmp." + quote(field, POSTGRES)).mkString(" and ")}
         |     where latest_current.${quote(primaryFields.head, POSTGRES)} is null
         |), update_data as (
         |    select ${currentTableColumnsList.map("tmp." + quote(_, POSTGRES)).mkString(", ")}
         |    from latest_current
         |--  此处的or语句中，需要把update_time过滤掉
         |    inner join ${quote(tempTable, POSTGRES)} tmp
         |    on ${primaryFields.map(field => "latest_current." + quote(field, POSTGRES) + " = tmp." + quote(field, POSTGRES)).mkString(" and ")}
         |    and (${currentTableColumnsList.diff(primaryFields).filterNot(_.equals(businessUpdateTime)).map(field => "latest_current." + quote(field, POSTGRES) + " != tmp." + quote(field, POSTGRES)).mkString(" or ")})
         |    and latest_current.${quote(businessUpdateTime, POSTGRES)} <= tmp.${quote(businessUpdateTime, POSTGRES)}
         |), insert_data_for_load as (
         |    select ${currentTableColumnsList.map(quote(_, POSTGRES)).mkString(", ")} from update_data
         |    union all
         |    select ${currentTableColumnsList.map(quote(_, POSTGRES)).mkString(", ")} from insert_data
         |)
         |insert into $currentTableName (${currentTableColumnsList.map(quote(_, POSTGRES)).mkString(", ")})
         |select ${currentTableColumnsList.map(quote(_, POSTGRES)).mkString(", ")} from insert_data_for_load;""".stripMargin
    conn.prepareStatement(deleteSql).execute()
    conn.prepareStatement(insertSql).execute()
  }

  private def clearResource(tempTable: String, conn: Connection): Unit = {
    val dropSql =
      s"""
         |DROP TABLE IF EXISTS $tempTable;""".stripMargin
    conn.prepareStatement(dropSql).execute()
  }

  def loadData(args: collection.Map[String, String]): Unit = {
    val slowChanging = args("slowChanging").toBoolean
    val updateType = args("updateType")

    val updateTable = args("updateTable")

    val currentDb = args("currentDb")
    val currentDbType = args("currentDbType")
    val currentTable = args("currentTable")

    val primaryFields = args("primaryFields").split(",").toList
    val businessCreateTime = args("businessCreateTime")
    val businessUpdateTime = args("businessUpdateTime")
    val currentTableColumnsAndType = args("currentTableColumnsAndType")

    val conn = JdbcConnection(currentDb, currentDbType).getConnection()

    val currentTableColumnsAndTypeJsonObj: JsonObject = new Gson().fromJson(currentTableColumnsAndType, classOf[JsonObject])
    val tempTable = loadTempDataToDb(conn, currentTableColumnsAndTypeJsonObj, currentDbType, currentDb, updateTable)

    val currentTableColumnsList = currentTableColumnsAndTypeJsonObj.entrySet().asScala.map(_.getKey).toSet.toList.sorted

    val currentTableName = s"${quote(currentDb, POSTGRES)}.${quote(currentTable, POSTGRES)}"
    if (slowChanging) {
      if (INCREMENTAL.equals(updateType)) {
        execSCIncremental(conn, currentTableName, primaryFields, tempTable,
          currentTableColumnsList, businessCreateTime, businessUpdateTime)
      } else {
        execSCFull(conn, currentTableName, primaryFields, tempTable,
          currentTableColumnsList, businessCreateTime, businessUpdateTime)
      }
    } else {
      if (INCREMENTAL.equals(updateType)) {
        execNoSCIncremental(conn, currentTableName, primaryFields, tempTable,
          currentTableColumnsList, businessUpdateTime)
      } else {
        execNoSCFull(conn, currentTableName, primaryFields, tempTable, currentTableColumnsList, businessUpdateTime)
      }
    }

    clearResource(tempTable, conn)
    conn.close()
  }

  override def transform(args: Map[String, String]): DataFrame = {
    loadData(args)
    sparkSession.emptyDataFrame
  }

}

// scalastyle:on
