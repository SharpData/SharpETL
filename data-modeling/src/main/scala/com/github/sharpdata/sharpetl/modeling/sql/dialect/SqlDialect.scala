package com.github.sharpdata.sharpetl.modeling.sql.dialect

import com.github.sharpdata.sharpetl.core.util.Constants.DataSourceType.{HIVE, INFORMIX, MS_SQL_SERVER, MYSQL, POSTGRES, SPARK_SQL}
import com.github.sharpdata.sharpetl.core.util.StringUtil.uuid

// scalastyle:off

/**
 * [[org.apache.spark.sql.jdbc.JdbcDialect]]
 */
sealed trait SqlDialect {
  def quoteIdentifier(colName: String): String

  def year(colName: String, timeFormat: String): String

  def month(colName: String, timeFormat: String): String

  def day(colName: String, timeFormat: String): String

  def hour(colName: String, timeFormat: String): String

  def minute(colName: String, timeFormat: String): String
}

case object HiveDialect extends SqlDialect {
  override def quoteIdentifier(colName: String): String = s"`$colName`"

  override def year(colName: String, timeFormat: String): String = s"from_unixtime(unix_timestamp($colName, '$timeFormat'), 'yyyy')"

  override def month(colName: String, timeFormat: String): String = s"from_unixtime(unix_timestamp($colName, '$timeFormat'), 'MM')"

  override def day(colName: String, timeFormat: String): String = s"from_unixtime(unix_timestamp($colName, '$timeFormat'), 'dd')"

  override def hour(colName: String, timeFormat: String): String = s"from_unixtime(unix_timestamp($colName, '$timeFormat'), 'HH')"

  override def minute(colName: String, timeFormat: String): String = s"from_unixtime(unix_timestamp($colName, '$timeFormat'), 'mm')"
}

case object MysqlDialect extends SqlDialect {
  override def quoteIdentifier(colName: String): String = s"`$colName`"

  override def year(colName: String, timeFormat: String): String = s"from_unixtime(unix_timestamp($colName), '%Y')"

  override def month(colName: String, timeFormat: String): String = s"from_unixtime(unix_timestamp($colName), '%m')"

  override def day(colName: String, timeFormat: String): String = s"from_unixtime(unix_timestamp($colName), '%d')"

  override def hour(colName: String, timeFormat: String): String = s"from_unixtime(unix_timestamp($colName), '%H')"

  override def minute(colName: String, timeFormat: String): String = s"from_unixtime(unix_timestamp($colName), '%i')"
}

case object PostgresDialect extends SqlDialect {
  override def quoteIdentifier(colName: String): String = {
    if (colName.contains(".")) {
      colName.split('.').map(col => s""""$col"""").mkString(".")
    } else {
      s""""$colName""""
    }
  }

  override def year(colName: String, timeFormat: String): String = s"""to_char(${quoteIdentifier(colName)}, 'yyyy')"""

  override def month(colName: String, timeFormat: String): String = s"""to_char(${quoteIdentifier(colName)}, 'MM')"""

  override def day(colName: String, timeFormat: String): String = s"""to_char(${quoteIdentifier(colName)}, 'DD')"""

  override def hour(colName: String, timeFormat: String): String = s"""to_char(${quoteIdentifier(colName)}, 'HH24')"""

  override def minute(colName: String, timeFormat: String): String = s"""to_char(${quoteIdentifier(colName)}, 'MI')"""
}

case object MSSqlDialect extends SqlDialect {
  override def quoteIdentifier(colName: String): String = s"[$colName]"

  override def year(colName: String, timeFormat: String): String = ???

  override def month(colName: String, timeFormat: String): String = ???

  override def day(colName: String, timeFormat: String): String = ???

  override def hour(colName: String, timeFormat: String): String = ???

  override def minute(colName: String, timeFormat: String): String = ???
}

case object InformixSqlDialect extends SqlDialect {
  override def quoteIdentifier(colName: String): String = colName

  override def year(colName: String, timeFormat: String): String = ???

  override def month(colName: String, timeFormat: String): String = ???

  override def day(colName: String, timeFormat: String): String = ???

  override def hour(colName: String, timeFormat: String): String = ???

  override def minute(colName: String, timeFormat: String): String = ???
}


object SqlDialect {
  def quote(name: String, `type`: String): String = quoteIdentifier(name, `type`)

  def quoteIdentifier(colName: String, `type`: String): String = {
    `type` match {
      case HIVE | SPARK_SQL => HiveDialect.quoteIdentifier(colName)
      case MYSQL => MysqlDialect.quoteIdentifier(colName)
      case POSTGRES => PostgresDialect.quoteIdentifier(colName)
      case MS_SQL_SERVER => MSSqlDialect.quoteIdentifier(colName)
      case INFORMIX => InformixSqlDialect.quoteIdentifier(colName)
    }
  }

  def surrogateKey(`type`: String): String = {
    `type` match {
      case POSTGRES => "uuid_generate_v1()"
      case _ => "uuid()"
    }
  }

  def getSqlDialect(`type`: String): SqlDialect = {
    `type` match {
      case HIVE | SPARK_SQL => HiveDialect
      case MYSQL => MysqlDialect
      case POSTGRES => PostgresDialect
      case MS_SQL_SERVER => MSSqlDialect
      case INFORMIX => InformixSqlDialect
    }
  }
}

// scalastyle:on
