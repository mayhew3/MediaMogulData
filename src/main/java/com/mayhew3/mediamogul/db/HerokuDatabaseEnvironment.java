package com.mayhew3.mediamogul.db;

import com.mayhew3.postgresobject.EnvironmentChecker;
import com.mayhew3.postgresobject.db.RemoteDatabaseEnvironment;
import com.mayhew3.postgresobject.exception.MissingEnvException;

public class HerokuDatabaseEnvironment extends RemoteDatabaseEnvironment {

  final String environmentVariableName;

  public HerokuDatabaseEnvironment(String environmentName, String environmentVariableName, Integer pgVersion, String herokuAppName) {
    super(environmentName, pgVersion, herokuAppName);
    this.environmentVariableName = environmentVariableName;
  }

  @Override
  public String getDatabaseUrl() throws MissingEnvException {
    ExecutionEnvironment thisEnvironment = ExecutionEnvironments.getThisEnvironment();
    if (thisEnvironment.isLocal) {
      return EnvironmentChecker.getOrThrow(environmentVariableName);
    } else {
      return EnvironmentChecker.getOrThrow("DATABASE_URL");
    }
  }

  @Override
  public boolean isLocal() {
    return false;
  }
}
