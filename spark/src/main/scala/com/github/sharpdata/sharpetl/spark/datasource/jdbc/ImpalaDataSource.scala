package com.github.sharpdata.sharpetl.spark.datasource.jdbc

import com.github.sharpdata.sharpetl.core.util.Constants.DataSourceType.IMPALA
import com.github.sharpdata.sharpetl.core.annotation._

@source(types = Array("impala"))
@sink(types = Array("impala"))
class ImpalaDataSource extends AbstractJdbcDataSource(IMPALA) {

  override def buildSelectSql(selectSql: String): String = s"($selectSql) as t"

}
