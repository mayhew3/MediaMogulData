package com.mayhew3.mediamogul.db;

import com.google.common.collect.Lists;
import com.mayhew3.mediamogul.ArgumentChecker;
import com.mayhew3.mediamogul.EnvironmentChecker;
import com.mayhew3.postgresobject.exception.MissingEnvException;

import java.util.Optional;

public enum ConnectionDetails {
  HEROKU("heroku", "postgresURL_heroku"),
  STAGING("staging", "postgresURL_heroku_staging"),
  LOCAL("local", "postgresURL_local"),
  TEST("test", "postgresURL_local_test"),
  E2E("e2e", "postgresURL_local_e2e");

  private String dbUrl;
  private String nickname;

  ConnectionDetails(String nickname, String envName) {
    try {
      String dbUrl = EnvironmentChecker.getOrThrow(envName);

      this.nickname = nickname;
      this.dbUrl = dbUrl;
    } catch (MissingEnvException e) {
      e.printStackTrace();
      throw new IllegalArgumentException(e);
    }
  }

  public static Optional<ConnectionDetails> getConnectionDetails(final String nickname) {
    return Lists.newArrayList(ConnectionDetails.values())
        .stream()
        .filter(connectionDetails -> connectionDetails.nickname.equalsIgnoreCase(nickname))
        .findAny();
  }

  public static ConnectionDetails getConnectionDetails(ArgumentChecker argumentChecker) {
    String dbIdentifier = argumentChecker.getDBIdentifier();
    Optional<ConnectionDetails> connectionDetails = getConnectionDetails(dbIdentifier);
    if (connectionDetails.isPresent()) {
      return connectionDetails.get();
    } else {
      throw new IllegalArgumentException("No Connection Details found for argument: " + dbIdentifier);
    }
  }

  public String getDbUrl() {
    return dbUrl;
  }

  public String getNickname() {
    return nickname;
  }
}
