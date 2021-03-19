package com.mayhew3.mediamogul.backup;

import com.mayhew3.mediamogul.GlobalConstants;
import com.mayhew3.mediamogul.db.*;
import com.mayhew3.mediamogul.scheduler.UpdateRunner;
import com.mayhew3.mediamogul.tv.helper.UpdateMode;
import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.db.DataBackupExecutor;
import com.mayhew3.postgresobject.db.DataBackupLocalExecutor;
import com.mayhew3.postgresobject.db.DataBackupRemoteExecutor;
import com.mayhew3.postgresobject.exception.MissingEnvException;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;

public class MediaMogulBackupExecutor implements UpdateRunner {

  private final DatabaseEnvironment databaseEnvironment;

  public static void main(String[] args) throws MissingEnvException, InterruptedException, IOException, com.mayhew3.mediamogul.exception.MissingEnvException {

    ArgumentChecker argumentChecker = new ArgumentChecker(args);
    argumentChecker.removeExpectedOption("db");
    argumentChecker.addExpectedOption("backupEnv", true, "Name of environment to backup (local, heroku, heroku-staging)");

    String backupEnv = argumentChecker.getRequiredValue("backupEnv");
    DatabaseEnvironment databaseEnvironment = DatabaseEnvironments.environments.get(backupEnv);

    MediaMogulBackupExecutor mediaMogulBackupExecutor = new MediaMogulBackupExecutor(databaseEnvironment);
    mediaMogulBackupExecutor.runUpdate();

  }

  public MediaMogulBackupExecutor(DatabaseEnvironment databaseEnvironment) {
    this.databaseEnvironment = databaseEnvironment;
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
    LocalDatabaseEnvironment localDatabaseEnvironment = (LocalDatabaseEnvironment) databaseEnvironment;
    String envName = localDatabaseEnvironment.getEnvironmentName();
    String localDBName = localDatabaseEnvironment.getDatabaseName();
    Integer pgVersion = localDatabaseEnvironment.getPgVersion();

    DataBackupExecutor executor = new DataBackupLocalExecutor(
        envName,
        pgVersion,
        GlobalConstants.appLabel,
        localDBName);
    executor.runUpdate();
  }

  private void updateRemote() throws MissingEnvException, IOException, InterruptedException {
    try {
      String envName = databaseEnvironment.getEnvironmentName();
      String databaseUrl = databaseEnvironment.getDatabaseUrl();
      Integer pgVersion = databaseEnvironment.getPgVersion();

      DataBackupExecutor executor = new DataBackupRemoteExecutor(
          envName,
          pgVersion,
          GlobalConstants.appLabel,
          databaseUrl);
      executor.runUpdate();
    } catch (com.mayhew3.mediamogul.exception.MissingEnvException e) {
      throw new MissingEnvException(e.getMessage());
    }
  }

}
