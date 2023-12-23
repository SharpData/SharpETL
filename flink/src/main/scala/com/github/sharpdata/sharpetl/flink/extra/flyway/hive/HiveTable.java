package com.github.sharpdata.sharpetl.flink.extra.flyway.hive;

import com.github.sharpdata.sharpetl.core.util.ETLConfig;
import com.github.sharpdata.sharpetl.flink.util.ETLFlinkSession;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;

import java.sql.SQLException;
import java.util.Arrays;

public class HiveTable extends Table<HiveDatabase, HiveSchema> {
    /**
     * @param jdbcTemplate The JDBC template for communicating with the DB.
     * @param database     The database-specific support.
     * @param schema       The schema this table lives in.
     * @param name         The name of the table.
     */
    public HiveTable(JdbcTemplate jdbcTemplate, HiveDatabase database, HiveSchema schema, String name) {
        super(jdbcTemplate, database, schema, name);
    }

    @Override
    protected boolean doExists() throws SQLException {
        return Arrays.stream(ETLFlinkSession.sparkSession().listTables(ETLConfig.getProperty("flyway.catalog"), ETLConfig.getProperty("flyway.database"))).anyMatch(it -> it.contentEquals(name));
        //return ETLFlinkSession.sparkSession().listTables(schema.getName(), name).length > 0;
        //return com.github.sharpdata.sharpetl.spark.utils.ETLSparkSession.getHiveSparkSession().catalog().tableExists(schema.getName(), name);
    }

    @Override
    protected void doLock() throws SQLException {

    }

    @Override
    protected void doDrop() throws SQLException {

    }
}
