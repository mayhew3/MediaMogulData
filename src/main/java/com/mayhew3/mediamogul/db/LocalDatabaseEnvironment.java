package com.mayhew3.mediamogul.db;

import com.mayhew3.mediamogul.EnvironmentChecker;
import com.mayhew3.mediamogul.exception.MissingEnvException;

public class LocalDatabaseEnvironment extends DatabaseEnvironment {

  final String databaseName;

  public LocalDatabaseEnvironment(String environmentName, String databaseName, Integer pgVersion) {
    super(environmentName, pgVersion);
    this.databaseName = databaseName;
  }

  @Override
  public String getDatabaseUrl() throws MissingEnvException {
    String localPassword = EnvironmentChecker.getOrThrow("postgres_local_password");
    int port = 5432 - 9 + pgVersion;
    return "jdbc:postgresql://localhost:" + port + "/" + databaseName + "?user=postgres&password=" + localPassword;
  }

  public String getDatabaseName() {
    return databaseName;
  }


  @Override
  public boolean isLocal() {
    return true;
  }
}
