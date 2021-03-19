package com.mayhew3.mediamogul.backup;

import com.mayhew3.mediamogul.GlobalConstants;
import com.mayhew3.mediamogul.db.DatabaseEnvironments;
import com.mayhew3.mediamogul.db.HerokuDatabaseEnvironment;
import com.mayhew3.mediamogul.scheduler.UpdateRunner;
import com.mayhew3.mediamogul.tv.helper.UpdateMode;
import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.db.*;
import com.mayhew3.postgresobject.exception.MissingEnvException;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;

public class MediaMogulBackupExecutor implements UpdateRunner {

  private final DatabaseEnvironment databaseEnvironment;

  public static void main(String[] args) throws InterruptedException, IOException, MissingEnvException {

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

    DataBackupExecutor executor = new DataBackupLocalExecutor(localDatabaseEnvironment, GlobalConstants.appLabel);
    executor.runUpdate();
  }

  private void updateRemote() throws MissingEnvException, IOException, InterruptedException {
    HerokuDatabaseEnvironment herokuDatabaseEnvironment = (HerokuDatabaseEnvironment) databaseEnvironment;

    DataBackupExecutor executor = new DataBackupRemoteExecutor(herokuDatabaseEnvironment, GlobalConstants.appLabel);
    executor.runUpdate();
  }

}
