package com.github.sharpdata.sharpetl.core.datasource.config

import com.github.sharpdata.sharpetl.core.annotation.configFor

@configFor(types = Array("delta_lake"))
class DeltaLakeDataSourceConfig extends DBDataSourceConfig with Serializable

