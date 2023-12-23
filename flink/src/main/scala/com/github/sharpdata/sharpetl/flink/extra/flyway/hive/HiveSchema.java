package com.github.sharpdata.sharpetl.flink.extra.flyway.hive;

import com.github.sharpdata.sharpetl.core.util.ETLConfig;
import com.github.sharpdata.sharpetl.flink.util.ETLFlinkSession;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.flywaydb.core.internal.database.base.Schema;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

public class HiveSchema extends Schema<HiveDatabase, Table> {
    /**
     * @param jdbcTemplate The Jdbc Template for communicating with the DB.
     * @param database     The database-specific support.
     * @param name         The name of the schema.
     */
    public HiveSchema(JdbcTemplate jdbcTemplate, HiveDatabase database, String name) {
        super(jdbcTemplate, database, name);
    }

    @Override
    protected boolean doExists() throws SQLException {
        final TableEnvironment session = ETLFlinkSession.sparkSession();
        ETLFlinkSession.createCatalogIfNeed("flink_sharp_etl", session);
        final Optional<Catalog> catalog = session.getCatalog(ETLConfig.getProperty("flyway.catalog"));
        catalog.get();
        return true;
        //return jdbcTemplate.queryForStringList("SHOW SCHEMAS").contains(name);
    }

    @Override
    protected boolean doEmpty() throws SQLException {
        return allTables().length == 0;
    }

    @Override
    protected void doCreate() throws SQLException {
        jdbcTemplate.execute("CREATE SCHEMA " + database.quote(name));
    }

    @Override
    protected void doDrop() throws SQLException {
        clean();
        jdbcTemplate.execute("DROP SCHEMA " + database.quote(name) + " RESTRICT");
    }

    @Override
    protected void doClean() throws SQLException {
        for (Table table : allTables())
            table.drop();
    }

    @Override
    protected Table[] doAllTables() throws SQLException {
        final String[] tableNames = ETLFlinkSession.sparkSession().listTables(ETLConfig.getProperty("flyway.catalog"), ETLConfig.getProperty("flyway.database"));

        Table[] tables = new Table[tableNames.length];
        for (int i = 0; i < tableNames.length; i++) {
            tables[i] = new HiveTable(jdbcTemplate, database, this, tableNames[i]);
        }
        return tables;
    }

    @Override
    public Table getTable(String tableName) {
        return new HiveTable(jdbcTemplate, database, this, tableName);
    }
}
