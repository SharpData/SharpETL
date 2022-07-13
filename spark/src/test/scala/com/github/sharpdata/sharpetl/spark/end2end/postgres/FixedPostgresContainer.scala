package com.github.sharpdata.sharpetl.spark.end2end.postgres

import org.testcontainers.containers.PostgreSQLContainer

class FixedPostgresContainer (val dockerImageName: String) extends PostgreSQLContainer(dockerImageName) {
  def configurePort(port: Int, dbName: String): FixedPostgresContainer = {
    super.addFixedExposedPort(port, 5432)
    super.withUsername("postgres")
    super.withPassword("postgres")
    super.withDatabaseName(dbName)
    this
  }
}