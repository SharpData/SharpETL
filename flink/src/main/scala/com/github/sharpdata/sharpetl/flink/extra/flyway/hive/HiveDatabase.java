package com.github.sharpdata.sharpetl.flink.extra.flyway.hive;

import com.github.sharpdata.sharpetl.core.util.ETLConfig;
import com.github.sharpdata.sharpetl.flink.util.ETLFlinkSession;
import org.flywaydb.core.api.CoreMigrationType;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.database.base.Database;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;
import org.flywaydb.core.internal.jdbc.StatementInterceptor;
import org.flywaydb.core.internal.util.AbbreviationUtils;

import java.sql.Connection;
import java.sql.SQLException;

public class HiveDatabase extends Database<HiveConnection> {
    public HiveDatabase(Configuration configuration, JdbcConnectionFactory jdbcConnectionFactory, StatementInterceptor statementInterceptor) {
        super(configuration, jdbcConnectionFactory, statementInterceptor);
    }

    @Override
    protected String doGetCatalog() throws SQLException {
        return ETLConfig.getProperty("flyway.catalog");
    }

    private String doGetDatabase() {
        return ETLConfig.getProperty("flyway.database");
    }

    @Override
    protected HiveConnection doGetConnection(Connection connection) {
        return new HiveConnection(this, connection);
    }

    @Override
    public void ensureSupported() {

    }

    @Override
    public boolean supportsDdlTransactions() {
        return false;
    }

    @Override
    public String getBooleanTrue() {
        return "true";
    }

    @Override
    public String getBooleanFalse() {
        return "false";
    }

    @Override
    public String doQuote(String identifier) {
        return "`" + identifier + "`";
    }

    @Override
    protected String getOpenQuote() {
        return "`";
    }

    @Override
    protected String getCloseQuote() {
        return "`";
    }

    @Override
    public String getEscapedQuote() {
        return "\\`";
    }

    @Override
    public boolean catalogIsSchema() {
        return true;
    }

    @Override
    public String getRawCreateScript(Table table, boolean baseline) {
        ETLFlinkSession.batchEnv().executeSql("create database if not exists " + doGetDatabase() + ";");
        return "CREATE TABLE " + table + " (\n" +
                "    `installed_rank` INT,\n" +
                "    `version` STRING,\n" +
                "    `description` STRING,\n" +
                "    `type` STRING,\n" +
                "    `script` STRING,\n" +
                "    `checksum` INT,\n" +
                "    `installed_by` STRING,\n" +
                "    `installed_on` TIMESTAMP,\n" +
                "    `execution_time` INT,\n" +
                "    `success` BOOLEAN\n" +
                ");\n" + baselineStatement(table) + ";\n";
    }

    @Override
    public String getSelectStatement(Table table) {
        return "SELECT " + quote("installed_rank")
                + "," + quote("version")
                + "," + quote("description")
                + "," + quote("type")
                + "," + quote("script")
                + "," + quote("checksum")
                + "," + quote("installed_on")
                + "," + quote("installed_by")
                + "," + quote("execution_time")
                + "," + quote("success")
                + " FROM " + table
                + " WHERE " + quote("installed_rank") + " > ?"
                + " ORDER BY " + quote("installed_rank");
    }

    @Override
    public String getInsertStatement(Table table) {
        // Explicitly set installed_on to CURRENT_TIMESTAMP().
        return "INSERT INTO " + table
                + " (" + quote("installed_rank")
                + ", " + quote("version")
                + ", " + quote("description")
                + ", " + quote("type")
                + ", " + quote("script")
                + ", " + quote("checksum")
                + ", " + quote("installed_by")
                + ", " + quote("installed_on")
                + ", " + quote("execution_time")
                + ", " + quote("success")
                + ")"
                + " VALUES (?, ?, ?, ?, ?, ?, ?, NOW(), ?, ?)";
    }

    public String baselineStatement(Table table) {
        return String.format(getInsertStatement(table).replace("?", "%s"),
                1,
                "'0'",
                "'" + AbbreviationUtils.abbreviateDescription(configuration.getBaselineDescription()) + "'",
                "'" + CoreMigrationType.BASELINE + "'",
                "'" + AbbreviationUtils.abbreviateScript(configuration.getBaselineDescription()) + "'",
                "0",
                "'" + getInstalledBy() + "'",
                0,
                getBooleanTrue()
        );
    }
}
