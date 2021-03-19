package com.mayhew3.mediamogul.db;

import com.mayhew3.mediamogul.EnvironmentChecker;
import com.mayhew3.mediamogul.exception.MissingEnvException;

public class HerokuDatabaseEnvironment extends DatabaseEnvironment {

  final String environmentVariableName;

  public HerokuDatabaseEnvironment(String environmentName, String environmentVariableName) {
    super(environmentName);
    this.environmentVariableName = environmentVariableName;
  }

  @Override
  public String getDatabaseUrl(ExecutionEnvironment executionEnvironment) throws MissingEnvException {
    if (executionEnvironment.isLocal) {
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
