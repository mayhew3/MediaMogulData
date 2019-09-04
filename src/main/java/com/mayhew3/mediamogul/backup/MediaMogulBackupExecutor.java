package com.mayhew3.mediamogul.backup;

import com.google.common.collect.Lists;
import com.mayhew3.mediamogul.scheduler.UpdateRunner;
import com.mayhew3.mediamogul.tv.helper.UpdateMode;
import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.EnvironmentChecker;
import com.mayhew3.postgresobject.db.DataBackupExecutor;
import com.mayhew3.postgresobject.db.DataBackupLocalExecutor;
import com.mayhew3.postgresobject.db.DataBackupRemoteExecutor;
import com.mayhew3.postgresobject.exception.MissingEnvException;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;

public class MediaMogulBackupExecutor implements UpdateRunner {

  private String backupEnv;

  public static void main(String[] args) throws MissingEnvException, InterruptedException, IOException {

    ArgumentChecker argumentChecker = new ArgumentChecker(args);
    argumentChecker.removeExpectedOption("db");
    argumentChecker.addExpectedOption("backupEnv", true, "Name of environment to backup (local, heroku, heroku-staging)");

    String backupEnv = argumentChecker.getRequiredValue("backupEnv");

    MediaMogulBackupExecutor mediaMogulBackupExecutor = new MediaMogulBackupExecutor(backupEnv);
    mediaMogulBackupExecutor.runUpdate();

  }

  public MediaMogulBackupExecutor(String backupEnv) {
    this.backupEnv = backupEnv;
  }

  @Override
  public String getRunnerName() {
    return "DB Backup Executor";
  }

  @Override
  public @Nullable UpdateMode getUpdateMode() {
    return null;
  }

  public void runUpdate() throws MissingEnvException, InterruptedException, IOException {
    if (isLocal()) {
      updateLocal();
    } else {
      updateRemote();
    }
  }

  private void updateLocal() throws MissingEnvException, InterruptedException, IOException {
    String localDBName = getLocalDBNameFromEnv(backupEnv);

    DataBackupExecutor executor = new DataBackupLocalExecutor(
        backupEnv,
        11,
        "MediaMogul",
        localDBName);
    executor.runUpdate();
  }

  private void updateRemote() throws MissingEnvException, IOException, InterruptedException {
    String databaseUrl = EnvironmentChecker.getOrThrow("DATABASE_URL");
    DataBackupExecutor executor = new DataBackupRemoteExecutor(
        backupEnv,
        11,
        "MediaMogul",
        databaseUrl);
    executor.runUpdate();
  }

  private boolean isLocal() {
    return Lists.newArrayList("local", "e2e").contains(backupEnv);
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

}
