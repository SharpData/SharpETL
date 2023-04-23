package com.github.sharpdata.sharpetl.spark.extra.flyway.hive;

import org.flywaydb.core.internal.database.base.Schema;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;

import java.util.List;

import java.sql.SQLException;

public class HiveSchema extends Schema<HiveDatabase, org.flywaydb.core.internal.database.base.Table> {
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
        return jdbcTemplate.queryForStringList("SHOW SCHEMAS").contains(name);
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
    protected org.flywaydb.core.internal.database.base.Table[] doAllTables() throws SQLException {
        List<String> tableNames = jdbcTemplate.queryForStringList("SHOW TABLES IN " + database.quote(name));

        Table[] tables = new Table[tableNames.size()];
        for (int i = 0; i < tableNames.size(); i++) {
            tables[i] = new HiveTable(jdbcTemplate, database, this, tableNames.get(i));
        }
        return tables;
    }

    @Override
    public Table getTable(String tableName) {
        return new HiveTable(jdbcTemplate, database, this, tableName);
    }
}
