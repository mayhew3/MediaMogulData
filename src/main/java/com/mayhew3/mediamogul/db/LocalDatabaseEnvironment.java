package com.mayhew3.mediamogul.db;

import com.mayhew3.mediamogul.EnvironmentChecker;
import com.mayhew3.mediamogul.exception.MissingEnvException;

public class LocalDatabaseEnvironment extends DatabaseEnvironment {

  final String databaseName;
  final Integer pgVersion;

  public LocalDatabaseEnvironment(String environmentName, String databaseName, Integer pgVersion) {
    super(environmentName);
    this.databaseName = databaseName;
    this.pgVersion = pgVersion;
  }

  @Override
  public String getDatabaseUrl(ExecutionEnvironment executionEnvironment) throws MissingEnvException {
    String localPassword = EnvironmentChecker.getOrThrow("postgres_local_password");
    int port = 5432 - 9 + pgVersion;
    return "jdbc:postgresql://localhost:" + port + "/" + databaseName + "?user=postgres&password=" + localPassword;
  }
}
