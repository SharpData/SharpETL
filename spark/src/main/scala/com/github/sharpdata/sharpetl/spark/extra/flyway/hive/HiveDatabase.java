package com.github.sharpdata.sharpetl.spark.extra.flyway.hive;

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
        return "sharp_etl";
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
        return "CREATE TABLE " + table + " (\n" +
                "    `installed_rank` INT NOT NULL,\n" +
                "    `version` STRING,\n" +
                "    `description` STRING NOT NULL,\n" +
                "    `type` STRING NOT NULL,\n" +
                "    `script` STRING NOT NULL,\n" +
                "    `checksum` INT,\n" +
                "    `installed_by` STRING NOT NULL,\n" +
                "    `installed_on` TIMESTAMP NOT NULL,\n" +
                "    `execution_time` INT NOT NULL,\n" +
                "    `success` BOOLEAN NOT NULL\n" +
                ");\n" +
                (baseline ? getInsertStatement(table) + ";\n" : "");
    }

    @Override
    public String getInsertStatement(Table table) {
        // Explicitly set installed_on to CURRENT_TIMESTAMP().
        return String.format("INSERT INTO " + table
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
                        + " VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP(), ?, ?)"
                        .replace("?", "%s"),
                1,
                "'" + configuration.getBaselineVersion() + "'",
                "'" + AbbreviationUtils.abbreviateDescription(configuration.getBaselineDescription()) + "'",
                "'" + CoreMigrationType.BASELINE + "'",
                "'" + AbbreviationUtils.abbreviateScript(configuration.getBaselineDescription()) + "'",
                "NULL",
                "'" + getInstalledBy() + "'",
                0,
                getBooleanTrue()
        );
    }
}
