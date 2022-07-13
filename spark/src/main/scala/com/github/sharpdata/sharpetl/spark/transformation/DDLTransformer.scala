package com.github.sharpdata.sharpetl.spark.transformation

import com.google.common.base.Strings.isNullOrEmpty
import com.github.sharpdata.sharpetl.core.util.{ETLConfig, ETLLogger, HDFSUtil}
import com.github.sharpdata.sharpetl.spark.datasource.connection.JdbcConnection
import com.github.sharpdata.sharpetl.spark.utils.ETLSparkSession
import org.apache.spark.sql.DataFrame

import java.sql.Connection
import java.sql.DriverManager

// $COVERAGE-OFF$
object DDLTransformer extends Transformer {

  override def transform(args: Map[String, String]): DataFrame = {
    val ddlPath = args.getOrElse("ddlPath", "/user/hive/sharp-etl/ddl")
    val hiveConn = getHiveJDBCConnection(ETLConfig.getProperty("hive.jdbc.url"))
    val pgConn = getPGJDBCConnection(args("dbName"), args("dbType"))
    val ddls = HDFSUtil.recursiveListFiles(ddlPath)
    try {
      ddls.foreach { ddl =>
        HDFSUtil.readLines(ddl).mkString("\n").split(";")
          .filterNot(it => isNullOrEmpty(it.trim))
          .foreach { sql =>
            val filePath = ddl.replace(ddlPath, "")
            if (filePath.contains("hive")) {
              ETLLogger.info(s"create hive table by $ddl")
              try {
                hiveConn.createStatement.execute(s"$sql\n")
              } catch {
                case e: Exception => ETLLogger.error(e.getMessage)
              }
            } else if (filePath.contains("yb") || filePath.contains("agg")) {
              ETLLogger.info(s"create yb table by $ddl")
              try {
                pgConn.createStatement.execute(s"$sql\n")
              } catch {
                case e: Exception => ETLLogger.error(e.getMessage)
              }
            } else {
              ETLLogger.error(s"Unknown ddl: $ddl")
            }
          }
      }
    } finally {
      hiveConn.close()
      pgConn.close()
    }
    ETLSparkSession.sparkSession.emptyDataFrame
  }

  private def getHiveJDBCConnection(jdbcUrl: String): Connection = {
    DriverManager.getConnection(jdbcUrl)
  }

  private def getPGJDBCConnection(dbName: String, dbType: String): Connection = {
    JdbcConnection(dbName, dbType).getConnection()
  }
}

// $COVERAGE-ON$
