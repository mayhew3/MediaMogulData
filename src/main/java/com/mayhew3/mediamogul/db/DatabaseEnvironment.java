package com.mayhew3.mediamogul.db;

import com.mayhew3.mediamogul.exception.MissingEnvException;

public abstract class DatabaseEnvironment {

  final String environmentName;
  final Integer pgVersion;

  public DatabaseEnvironment(String environmentName, Integer pgVersion) {
    this.environmentName = environmentName;
    this.pgVersion = pgVersion;
  }

  public String getEnvironmentName() {
    return environmentName;
  }

  public Integer getPgVersion() {
    return pgVersion;
  }

  public abstract String getDatabaseUrl() throws MissingEnvException;
  public abstract boolean isLocal();
}
