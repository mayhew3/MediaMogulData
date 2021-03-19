package com.mayhew3.mediamogul.db;

import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.db.DatabaseEnvironment;
import com.mayhew3.postgresobject.db.LocalDatabaseEnvironment;

import java.util.HashMap;
import java.util.Map;

public class DatabaseEnvironments {
  public static Map<String, DatabaseEnvironment> environments = new HashMap<>();

  static {
    addLocal("local", "tv", 13);
    addLocal("test", "tv_copy", 13);
    addLocal("e2e", "tv_e2e", 13);
    addHeroku("heroku", "postgresURL_mediamogul", 11, "media-mogul");
    addHeroku("heroku-staging", "postgresURL_mediamogul_staging", 11, "media-mogul-staging");
  }

  @SuppressWarnings("SameParameterValue")
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

  public static DatabaseEnvironment getEnvironmentForDBArgument(ArgumentChecker argumentChecker) {
    String dbIdentifier = argumentChecker.getDBIdentifier();
    DatabaseEnvironment databaseEnvironment = environments.get(dbIdentifier);
    if (databaseEnvironment == null) {
      throw new IllegalArgumentException("No environment found with name: " + dbIdentifier);
    }
    return databaseEnvironment;
  }
}
