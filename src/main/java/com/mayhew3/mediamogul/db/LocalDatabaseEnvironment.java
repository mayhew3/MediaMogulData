package com.mayhew3.mediamogul.db;

import com.mayhew3.postgresobject.EnvironmentChecker;
import com.mayhew3.postgresobject.exception.MissingEnvException;

public class LocalDatabaseEnvironment extends DatabaseEnvironment {

  final String databaseName;
  final Integer port;

  public LocalDatabaseEnvironment(String environmentName, String databaseName, Integer port, Integer pgVersion) {
    super(environmentName, pgVersion);
    this.databaseName = databaseName;
    this.port = port;
  }

  @Override
  public String getDatabaseUrl() throws MissingEnvException {
    String localPassword = EnvironmentChecker.getOrThrow("postgres_local_password");
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
