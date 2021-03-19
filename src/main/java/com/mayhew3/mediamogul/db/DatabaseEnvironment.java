package com.mayhew3.mediamogul.db;

import com.mayhew3.mediamogul.exception.MissingEnvException;

public abstract class DatabaseEnvironment {

  final String environmentName;

  public DatabaseEnvironment(String environmentName) {
    this.environmentName = environmentName;
  }

  public String getEnvironmentName() {
    return environmentName;
  }

  public abstract String getDatabaseUrl(ExecutionEnvironment executionEnvironment) throws MissingEnvException;
  public abstract boolean isLocal();
}
