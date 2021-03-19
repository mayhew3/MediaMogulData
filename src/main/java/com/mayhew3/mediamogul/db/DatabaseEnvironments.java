package com.mayhew3.mediamogul.db;

import java.util.HashMap;
import java.util.Map;

public class DatabaseEnvironments {
  public static Map<String, DatabaseEnvironment> environments = new HashMap<>();

  static {
    addLocal("local", "tv", 11);
    addLocal("local_13", "tv", 13);
    addLocal("test", "tv_copy", 11);
    addLocal("e2e", "tv_e2e", 11);
    addHeroku("heroku", "postgresURL_heroku", 11, "media-mogul");
    addHeroku("heroku-staging", "postgresURL_heroku_staging", 11, "media-mogul-staging");
  }

  private static void addLocal(String environmentName, String databaseName, Integer pgVersion) {
    Integer port = 5432 - 9 + pgVersion;
    LocalDatabaseEnvironment local = new LocalDatabaseEnvironment(environmentName, databaseName, port, pgVersion);
    environments.put(environmentName, local);
  }

  @SuppressWarnings("SameParameterValue")
  private static void addHeroku(String environmentName, String databaseName, Integer pgVersion, String herokuAppName) {
    HerokuDatabaseEnvironment local = new HerokuDatabaseEnvironment(environmentName, databaseName, pgVersion, herokuAppName);
    environments.put(environmentName, local);
  }

}
