package com.mayhew3.mediamogul.db;

import com.mayhew3.mediamogul.EnvironmentChecker;
import com.mayhew3.mediamogul.exception.MissingEnvException;
import com.mayhew3.postgresobject.ArgumentChecker;

public class MediaMogulDatabaseDetails {
  public static String localDBName = "tv";
  public static String debugDBName = "tv_copy";
  public static String e2eDBName = "tv_e2e";

  public static String herokuUrl = System.getenv("postgresURL_heroku");
  public static String herokuStagingUrl = System.getenv("postgresURL_heroku_staging");

  public static String getLocalUrl(int pgVersion) throws MissingEnvException {
    String localPassword = EnvironmentChecker.getOrThrow("postgres_local_password");
    int port = 5432 - 9 + pgVersion;
    return "jdbc:postgresql://localhost:" + port + "/" + localDBName + "?user=postgres&password=" + localPassword;
  }

  public static String getDatabaseUrl(ArgumentChecker argumentChecker, int localPgVersion) throws MissingEnvException {
    
  }
}
