package com.github.sharpdata.sharpetl.spark.extra.flyway.hive;

import org.flywaydb.core.internal.database.base.Connection;
import org.flywaydb.core.internal.database.base.Schema;

import java.sql.SQLException;

public class HiveConnection extends Connection<HiveDatabase> {
    protected HiveConnection(HiveDatabase database, java.sql.Connection connection) {
        super(database, connection);
    }

    @Override
    protected String getCurrentSchemaNameOrSearchPath() throws SQLException {
        return jdbcTemplate.queryForString("SELECT current_database()");
    }

    @Override
    public Schema getSchema(String name) {
        return new HiveSchema(this.getJdbcTemplate(), this.database, name);
    }
}
