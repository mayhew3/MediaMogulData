package com.mayhew3.mediamogul.db;

import com.mayhew3.postgresobject.EnvironmentChecker;
import com.mayhew3.postgresobject.exception.MissingEnvException;

public class HerokuDatabaseEnvironment extends DatabaseEnvironment {

  final String environmentVariableName;
  final String herokuAppName;

  public HerokuDatabaseEnvironment(String environmentName, String environmentVariableName, Integer pgVersion, String herokuAppName) {
    super(environmentName, pgVersion);
    this.environmentVariableName = environmentVariableName;
    this.herokuAppName = herokuAppName;
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

  public String getHerokuAppName() {
    return herokuAppName;
  }

  @Override
  public boolean isLocal() {
    return false;
  }
}
