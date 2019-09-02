package com.mayhew3.mediamogul.backup;

import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.db.DataRestoreExecutor;
import com.mayhew3.postgresobject.exception.MissingEnvException;

import java.io.IOException;
import java.util.Optional;

public class MediaMogulRestoreExecutor {

  public static void main(String... args) throws MissingEnvException, InterruptedException, IOException {

    ArgumentChecker argumentChecker = new ArgumentChecker(args);
    argumentChecker.removeExpectedOption("db");
    argumentChecker.addExpectedOption("backupEnv", true, "Name of environment to backup (local, heroku, heroku-staging)");
    argumentChecker.addExpectedOption("restoreEnv", true, "Name of environment to restore (local, heroku, heroku-staging)");

    String backupEnv = argumentChecker.getRequiredValue("backupEnv");
    String restoreEnv = argumentChecker.getRequiredValue("restoreEnv");

    Optional<String> localDBName = getLocalDBNameFromEnv(restoreEnv);
    Optional<String> appNameFromEnv = getAppNameFromEnv(restoreEnv);

    DataRestoreExecutor dataRestoreExecutor = new DataRestoreExecutor(
        restoreEnv,
        backupEnv,
        appNameFromEnv,
        9,
        "MediaMogul",
        localDBName);
    dataRestoreExecutor.runUpdate();
  }

  private static Optional<String> getLocalDBNameFromEnv(String backupEnv) {
    if ("local".equalsIgnoreCase(backupEnv)) {
      return Optional.of("tv");
    } else if ("e2e".equalsIgnoreCase(backupEnv)) {
      return Optional.of("tv_e2e");
    } else {
      return Optional.empty();
    }
  }

  private static Optional<String> getAppNameFromEnv(String backupEnv) {
    if ("heroku".equalsIgnoreCase(backupEnv)) {
      return Optional.of("media-mogul");
    } else if ("heroku-staging".equalsIgnoreCase(backupEnv)) {
      return Optional.of("media-mogul-staging");
    } else {
      return Optional.empty();
    }
  }

}
