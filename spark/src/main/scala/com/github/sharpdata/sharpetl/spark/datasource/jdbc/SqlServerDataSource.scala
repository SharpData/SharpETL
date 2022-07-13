package com.github.sharpdata.sharpetl.spark.datasource.jdbc

import com.github.sharpdata.sharpetl.core.util.Constants.DataSourceType.MS_SQL_SERVER
import com.github.sharpdata.sharpetl.core.annotation._

@source(types = Array("ms_sql_server"))
@sink(types = Array("ms_sql_server"))
class SqlServerDataSource extends AbstractJdbcDataSource(MS_SQL_SERVER) {

  override def buildSelectSql(selectSql: String): String = s"($selectSql) as t"

}
