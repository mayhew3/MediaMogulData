package com.mayhew3.mediamogul.db;

public class ExecutionEnvironment {

  final String environmentName;
  final boolean isLocal;
  final String appRole;

  public ExecutionEnvironment(String environmentName, boolean isLocal, String appRole) {
    this.environmentName = environmentName;
    this.isLocal = isLocal;
    this.appRole = appRole;
  }

  public boolean isLocal() {
    return isLocal;
  }

  public String getAppRole() {
    return appRole;
  }
}
