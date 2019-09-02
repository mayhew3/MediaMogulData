package com.mayhew3.mediamogul.backup;

import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.db.DataBackupExecutor;
import com.mayhew3.postgresobject.exception.MissingEnvException;

import java.io.IOException;
import java.util.Optional;

public class MediaMogulBackupExecutor {

  public static void main(String[] args) throws MissingEnvException, InterruptedException, IOException {

    ArgumentChecker argumentChecker = new ArgumentChecker(args);
    argumentChecker.removeExpectedOption("db");
    argumentChecker.addExpectedOption("backupEnv", true, "Name of environment to backup (local, heroku, heroku-staging)");

    String backupEnv = argumentChecker.getRequiredValue("backupEnv");

    Optional<String> localDBName = getLocalDBNameFromEnv(backupEnv);

    DataBackupExecutor executor = new DataBackupExecutor(
        backupEnv,
        9,
        "MediaMogul",
        localDBName);
    executor.runUpdate();
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

}
