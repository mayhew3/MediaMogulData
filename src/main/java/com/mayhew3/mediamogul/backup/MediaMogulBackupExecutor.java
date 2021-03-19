package com.mayhew3.mediamogul.backup;

import com.mayhew3.mediamogul.db.*;
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

  private final DatabaseEnvironment databaseEnvironment;
  private final ExecutionEnvironment executionEnvironment;

  public static void main(String[] args) throws MissingEnvException, InterruptedException, IOException, com.mayhew3.mediamogul.exception.MissingEnvException {

    ExecutionEnvironment executionEnvironment = ExecutionEnvironments.getThisEnvironment();

    ArgumentChecker argumentChecker = new ArgumentChecker(args);
    argumentChecker.removeExpectedOption("db");
    argumentChecker.addExpectedOption("backupEnv", true, "Name of environment to backup (local, heroku, heroku-staging)");

    String backupEnv = argumentChecker.getRequiredValue("backupEnv");
    DatabaseEnvironment databaseEnvironment = DatabaseEnvironments.environments.get(backupEnv);

    MediaMogulBackupExecutor mediaMogulBackupExecutor = new MediaMogulBackupExecutor(databaseEnvironment, executionEnvironment);
    mediaMogulBackupExecutor.runUpdate();

  }

  public MediaMogulBackupExecutor(DatabaseEnvironment databaseEnvironment, ExecutionEnvironment executionEnvironment) {
    this.databaseEnvironment = databaseEnvironment;
    this.executionEnvironment = executionEnvironment;
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
    if (databaseEnvironment.isLocal()) {
      updateLocal();
    } else {
      updateRemote();
    }
  }

  private void updateLocal() throws MissingEnvException, InterruptedException, IOException {
    String envName = databaseEnvironment.getEnvironmentName();
    String localDBName = ((LocalDatabaseEnvironment)databaseEnvironment).getDatabaseName();

    DataBackupExecutor executor = new DataBackupLocalExecutor(
        envName,
        11,
        "MediaMogul",
        localDBName);
    executor.runUpdate();
  }

  private void updateRemote() throws MissingEnvException, IOException, InterruptedException {
    try {
      String envName = databaseEnvironment.getEnvironmentName();
      String databaseUrl = databaseEnvironment.getDatabaseUrl(executionEnvironment);
      DataBackupExecutor executor = new DataBackupRemoteExecutor(
          envName,
          11,
          "MediaMogul",
          databaseUrl);
      executor.runUpdate();
    } catch (com.mayhew3.mediamogul.exception.MissingEnvException e) {
      throw new MissingEnvException(e.getMessage());
    }
  }

}
