package com.github.sharpdata.sharpetl.spark.extra.driver

import com.github.sharpdata.sharpetl.spark.utils.ETLSparkSession.sparkSession

import java.sql.{Connection, DatabaseMetaData, ResultSet, RowIdLifetime}

class SparkDatabaseMetaData extends DatabaseMetaData {
  override def allProceduresAreCallable(): Boolean = true

  override def allTablesAreSelectable(): Boolean = true

  override def getURL: String = ""

  override def getUserName: String = ""

  override def isReadOnly: Boolean = false

  override def nullsAreSortedHigh(): Boolean = true

  override def nullsAreSortedLow(): Boolean = false

  override def nullsAreSortedAtStart(): Boolean = true

  override def nullsAreSortedAtEnd(): Boolean = false

  override def getDatabaseProductName: String = "spark_sharp_etl"

  override def getDatabaseProductVersion: String = sparkSession.version

  override def getDriverName: String = "com.github.sharpdata.sharpetl.spark.extra.driver.SparkJdbcDriver"

  override def getDriverVersion: String = "0"

  override def getDriverMajorVersion: Int = 0

  override def getDriverMinorVersion: Int = 0

  override def usesLocalFiles(): Boolean = true

  override def usesLocalFilePerTable(): Boolean = true

  override def supportsMixedCaseIdentifiers(): Boolean = true

  override def storesUpperCaseIdentifiers(): Boolean = true

  override def storesLowerCaseIdentifiers(): Boolean = true

  override def storesMixedCaseIdentifiers(): Boolean = true

  override def supportsMixedCaseQuotedIdentifiers(): Boolean = true

  override def storesUpperCaseQuotedIdentifiers(): Boolean = true

  override def storesLowerCaseQuotedIdentifiers(): Boolean = true

  override def storesMixedCaseQuotedIdentifiers(): Boolean = true

  override def getIdentifierQuoteString: String = "`"

  override def getSQLKeywords: String = ???

  override def getNumericFunctions: String = ???

  override def getStringFunctions: String = ???

  override def getSystemFunctions: String = ???

  override def getTimeDateFunctions: String = ???

  override def getSearchStringEscape: String = ???

  override def getExtraNameCharacters: String = ???

  override def supportsAlterTableWithAddColumn(): Boolean = true

  override def supportsAlterTableWithDropColumn(): Boolean = true

  override def supportsColumnAliasing(): Boolean = true

  override def nullPlusNonNullIsNull(): Boolean = true

  override def supportsConvert(): Boolean = true

  override def supportsConvert(fromType: Int, toType: Int): Boolean = true

  override def supportsTableCorrelationNames(): Boolean = true

  override def supportsDifferentTableCorrelationNames(): Boolean = true

  override def supportsExpressionsInOrderBy(): Boolean = true

  override def supportsOrderByUnrelated(): Boolean = true

  override def supportsGroupBy(): Boolean = true

  override def supportsGroupByUnrelated(): Boolean = true

  override def supportsGroupByBeyondSelect(): Boolean = true

  override def supportsLikeEscapeClause(): Boolean = true

  override def supportsMultipleResultSets(): Boolean = true

  override def supportsMultipleTransactions(): Boolean = true

  override def supportsNonNullableColumns(): Boolean = true

  override def supportsMinimumSQLGrammar(): Boolean = true

  override def supportsCoreSQLGrammar(): Boolean = true

  override def supportsExtendedSQLGrammar(): Boolean = true

  override def supportsANSI92EntryLevelSQL(): Boolean = true

  override def supportsANSI92IntermediateSQL(): Boolean = true

  override def supportsANSI92FullSQL(): Boolean = true

  override def supportsIntegrityEnhancementFacility(): Boolean = true

  override def supportsOuterJoins(): Boolean = true

  override def supportsFullOuterJoins(): Boolean = true

  override def supportsLimitedOuterJoins(): Boolean = true

  override def getSchemaTerm: String = ???

  override def getProcedureTerm: String = ???

  override def getCatalogTerm: String = ???

  override def isCatalogAtStart: Boolean = true

  override def getCatalogSeparator: String = ???

  override def supportsSchemasInDataManipulation(): Boolean = true

  override def supportsSchemasInProcedureCalls(): Boolean = true

  override def supportsSchemasInTableDefinitions(): Boolean = true

  override def supportsSchemasInIndexDefinitions(): Boolean = true

  override def supportsSchemasInPrivilegeDefinitions(): Boolean = true

  override def supportsCatalogsInDataManipulation(): Boolean = true

  override def supportsCatalogsInProcedureCalls(): Boolean = true

  override def supportsCatalogsInTableDefinitions(): Boolean = true

  override def supportsCatalogsInIndexDefinitions(): Boolean = true

  override def supportsCatalogsInPrivilegeDefinitions(): Boolean = true

  override def supportsPositionedDelete(): Boolean = true

  override def supportsPositionedUpdate(): Boolean = true

  override def supportsSelectForUpdate(): Boolean = true

  override def supportsStoredProcedures(): Boolean = true

  override def supportsSubqueriesInComparisons(): Boolean = true

  override def supportsSubqueriesInExists(): Boolean = true

  override def supportsSubqueriesInIns(): Boolean = true

  override def supportsSubqueriesInQuantifieds(): Boolean = true

  override def supportsCorrelatedSubqueries(): Boolean = true

  override def supportsUnion(): Boolean = true

  override def supportsUnionAll(): Boolean = true

  override def supportsOpenCursorsAcrossCommit(): Boolean = true

  override def supportsOpenCursorsAcrossRollback(): Boolean = true

  override def supportsOpenStatementsAcrossCommit(): Boolean = true

  override def supportsOpenStatementsAcrossRollback(): Boolean = true

  override def getMaxBinaryLiteralLength: Int = 0

  override def getMaxCharLiteralLength: Int = 0

  override def getMaxColumnNameLength: Int = 0

  override def getMaxColumnsInGroupBy: Int = 0

  override def getMaxColumnsInIndex: Int = 0

  override def getMaxColumnsInOrderBy: Int = 0

  override def getMaxColumnsInSelect: Int = 0

  override def getMaxColumnsInTable: Int = 0

  override def getMaxConnections: Int = 0

  override def getMaxCursorNameLength: Int = 0

  override def getMaxIndexLength: Int = 0

  override def getMaxSchemaNameLength: Int = 0

  override def getMaxProcedureNameLength: Int = 0

  override def getMaxCatalogNameLength: Int = 0

  override def getMaxRowSize: Int = 0

  override def doesMaxRowSizeIncludeBlobs(): Boolean = true

  override def getMaxStatementLength: Int = 0

  override def getMaxStatements: Int = 0

  override def getMaxTableNameLength: Int = 0

  override def getMaxTablesInSelect: Int = 0

  override def getMaxUserNameLength: Int = 0

  override def getDefaultTransactionIsolation: Int = 0

  override def supportsTransactions(): Boolean = true

  override def supportsTransactionIsolationLevel(level: Int): Boolean = true

  override def supportsDataDefinitionAndDataManipulationTransactions(): Boolean = true

  override def supportsDataManipulationTransactionsOnly(): Boolean = true

  override def dataDefinitionCausesTransactionCommit(): Boolean = true

  override def dataDefinitionIgnoredInTransactions(): Boolean = true

  override def getProcedures(catalog: String, schemaPattern: String, procedureNamePattern: String): ResultSet = ???

  override def getProcedureColumns(catalog: String, schemaPattern: String, procedureNamePattern: String, columnNamePattern: String): ResultSet = ???

  override def getTables(catalog: String, schemaPattern: String, tableNamePattern: String, types: Array[String]): ResultSet = ???

  override def getSchemas: ResultSet = ???

  override def getCatalogs: ResultSet = ???

  override def getTableTypes: ResultSet = ???

  override def getColumns(catalog: String, schemaPattern: String, tableNamePattern: String, columnNamePattern: String): ResultSet = ???

  override def getColumnPrivileges(catalog: String, schema: String, table: String, columnNamePattern: String): ResultSet = ???

  override def getTablePrivileges(catalog: String, schemaPattern: String, tableNamePattern: String): ResultSet = ???

  override def getBestRowIdentifier(catalog: String, schema: String, table: String, scope: Int, nullable: Boolean): ResultSet = ???

  override def getVersionColumns(catalog: String, schema: String, table: String): ResultSet = ???

  override def getPrimaryKeys(catalog: String, schema: String, table: String): ResultSet = ???

  override def getImportedKeys(catalog: String, schema: String, table: String): ResultSet = ???

  override def getExportedKeys(catalog: String, schema: String, table: String): ResultSet = ???

  override def getCrossReference(parentCatalog: String, parentSchema: String, parentTable: String, foreignCatalog: String, foreignSchema: String, foreignTable: String): ResultSet = ???

  override def getTypeInfo: ResultSet = ???

  override def getIndexInfo(catalog: String, schema: String, table: String, unique: Boolean, approximate: Boolean): ResultSet = ???

  override def supportsResultSetType(`type`: Int): Boolean = true

  override def supportsResultSetConcurrency(`type`: Int, concurrency: Int): Boolean = true

  override def ownUpdatesAreVisible(`type`: Int): Boolean = true

  override def ownDeletesAreVisible(`type`: Int): Boolean = true

  override def ownInsertsAreVisible(`type`: Int): Boolean = true

  override def othersUpdatesAreVisible(`type`: Int): Boolean = true

  override def othersDeletesAreVisible(`type`: Int): Boolean = true

  override def othersInsertsAreVisible(`type`: Int): Boolean = true

  override def updatesAreDetected(`type`: Int): Boolean = true

  override def deletesAreDetected(`type`: Int): Boolean = true

  override def insertsAreDetected(`type`: Int): Boolean = true

  override def supportsBatchUpdates(): Boolean = true

  override def getUDTs(catalog: String, schemaPattern: String, typeNamePattern: String, types: Array[Int]): ResultSet = ???

  override def getConnection: Connection = ???

  override def supportsSavepoints(): Boolean = true

  override def supportsNamedParameters(): Boolean = true

  override def supportsMultipleOpenResults(): Boolean = true

  override def supportsGetGeneratedKeys(): Boolean = true

  override def getSuperTypes(catalog: String, schemaPattern: String, typeNamePattern: String): ResultSet = ???

  override def getSuperTables(catalog: String, schemaPattern: String, tableNamePattern: String): ResultSet = ???

  override def getAttributes(catalog: String, schemaPattern: String, typeNamePattern: String, attributeNamePattern: String): ResultSet = ???

  override def supportsResultSetHoldability(holdability: Int): Boolean = true

  override def getResultSetHoldability: Int = 0

  override def getDatabaseMajorVersion: Int = 0

  override def getDatabaseMinorVersion: Int = 0

  override def getJDBCMajorVersion: Int = 0

  override def getJDBCMinorVersion: Int = 0

  override def getSQLStateType: Int = 0

  override def locatorsUpdateCopy(): Boolean = true

  override def supportsStatementPooling(): Boolean = true

  override def getRowIdLifetime: RowIdLifetime = ???

  override def getSchemas(catalog: String, schemaPattern: String): ResultSet = ???

  override def supportsStoredFunctionsUsingCallSyntax(): Boolean = true

  override def autoCommitFailureClosesAllResultSets(): Boolean = true

  override def getClientInfoProperties: ResultSet = ???

  override def getFunctions(catalog: String, schemaPattern: String, functionNamePattern: String): ResultSet = ???

  override def getFunctionColumns(catalog: String, schemaPattern: String, functionNamePattern: String, columnNamePattern: String): ResultSet = ???

  override def getPseudoColumns(catalog: String, schemaPattern: String, tableNamePattern: String, columnNamePattern: String): ResultSet = ???

  override def generatedKeyAlwaysReturned(): Boolean = true

  override def unwrap[T](iface: Class[T]): T = ???

  override def isWrapperFor(iface: Class[_]): Boolean = true
}
