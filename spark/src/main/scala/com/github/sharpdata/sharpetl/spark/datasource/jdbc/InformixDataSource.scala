package com.github.sharpdata.sharpetl.spark.datasource.jdbc

import com.github.sharpdata.sharpetl.core.util.Constants.DataSourceType.INFORMIX
import com.github.sharpdata.sharpetl.core.annotation._

@source(types = Array("informix"))
@sink(types = Array("informix"))
class InformixDataSource extends AbstractJdbcDataSource(INFORMIX)
