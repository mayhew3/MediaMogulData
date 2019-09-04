package com.mayhew3.mediamogul.backup;

import com.google.common.collect.Lists;
import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.EnvironmentChecker;
import com.mayhew3.postgresobject.db.DataRestoreExecutor;
import com.mayhew3.postgresobject.db.DataRestoreLocalExecutor;
import com.mayhew3.postgresobject.db.DataRestoreRemoteExecutor;
import com.mayhew3.postgresobject.exception.MissingEnvException;

import java.io.IOException;

public class MediaMogulRestoreExecutor {

  private static String restoreEnv;

  public static void main(String... args) throws MissingEnvException, InterruptedException, IOException {

    ArgumentChecker argumentChecker = new ArgumentChecker(args);
    argumentChecker.removeExpectedOption("db");
    argumentChecker.addExpectedOption("backupEnv", true, "Name of environment to backup (local, heroku, heroku-staging)");
    argumentChecker.addExpectedOption("restoreEnv", true, "Name of environment to restore (local, heroku, heroku-staging)");

    String backupEnv = argumentChecker.getRequiredValue("backupEnv");
    restoreEnv = argumentChecker.getRequiredValue("restoreEnv");

    if (isLocal()) {
      String localDBName = getLocalDBNameFromEnv(restoreEnv);
      DataRestoreExecutor dataRestoreExecutor = new DataRestoreLocalExecutor(
          restoreEnv,
          backupEnv,
          11,
          "MediaMogul",
          localDBName);
      dataRestoreExecutor.runUpdate();
    } else {
      String appNameFromEnv = getAppNameFromEnv(restoreEnv);
      String databaseUrl = EnvironmentChecker.getOrThrow("DATABASE_URL");
      DataRestoreExecutor dataRestoreExecutor = new DataRestoreRemoteExecutor(
          restoreEnv,
          backupEnv,
          11,
          "MediaMogul",
          appNameFromEnv,
          databaseUrl);
      dataRestoreExecutor.runUpdate();
    }
  }

  private static boolean isLocal() {
    return Lists.newArrayList("local", "e2e").contains(restoreEnv);
  }

  private static String getLocalDBNameFromEnv(String backupEnv) {
    if ("local".equalsIgnoreCase(backupEnv)) {
      return "tv";
    } else if ("e2e".equalsIgnoreCase(backupEnv)) {
      return "tv_e2e";
    } else {
      return null;
    }
  }

  private static String getAppNameFromEnv(String backupEnv) {
    if ("heroku".equalsIgnoreCase(backupEnv)) {
      return "media-mogul";
    } else if ("heroku-staging".equalsIgnoreCase(backupEnv)) {
      return "media-mogul-staging";
    } else {
      return null;
    }
  }

}
