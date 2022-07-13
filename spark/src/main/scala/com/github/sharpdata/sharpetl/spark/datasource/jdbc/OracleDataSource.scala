package com.github.sharpdata.sharpetl.spark.datasource.jdbc

import com.github.sharpdata.sharpetl.core.util.Constants.DataSourceType.ORACLE
import com.github.sharpdata.sharpetl.core.annotation._

@source(types = Array("oracle"))
@sink(types = Array("oracle"))
class OracleDataSource extends AbstractJdbcDataSource(ORACLE)
