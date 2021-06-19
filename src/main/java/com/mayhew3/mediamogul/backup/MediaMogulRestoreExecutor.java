package com.mayhew3.mediamogul.backup;

import com.mayhew3.mediamogul.GlobalConstants;
import com.mayhew3.postgresobject.db.DatabaseEnvironment;
import com.mayhew3.mediamogul.db.DatabaseEnvironments;
import com.mayhew3.mediamogul.db.HerokuDatabaseEnvironment;
import com.mayhew3.postgresobject.db.LocalDatabaseEnvironment;
import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.db.DataRestoreExecutor;
import com.mayhew3.postgresobject.db.DataRestoreLocalExecutor;
import com.mayhew3.postgresobject.db.DataRestoreRemoteExecutor;
import com.mayhew3.postgresobject.exception.MissingEnvException;
import org.joda.time.DateTime;

import java.io.IOException;

public class MediaMogulRestoreExecutor {

  private static final DateTime backupDate = new DateTime(2021, 4, 30, 23, 30, 0);

  private final DatabaseEnvironment backupEnvironment;
  private final DatabaseEnvironment restoreEnvironment;
  private final boolean oldBackup;

  public static void main(String... args) throws InterruptedException, IOException, com.mayhew3.postgresobject.exception.MissingEnvException {

    ArgumentChecker argumentChecker = new ArgumentChecker(args);
    argumentChecker.removeExpectedOption("db");
    argumentChecker.addExpectedOption("backupEnv", true, "Name of environment to backup (local, heroku, heroku-staging)");
    argumentChecker.addExpectedOption("restoreEnv", true, "Name of environment to restore (local, heroku, heroku-staging)");
    argumentChecker.addExpectedOption("oldBackup", true, "Whether to restore older backup, using date above.");

    String backupEnv = argumentChecker.getRequiredValue("backupEnv");
    String restoreEnv = argumentChecker.getRequiredValue("restoreEnv");
    boolean oldBackup = Boolean.parseBoolean(argumentChecker.getRequiredValue("oldBackup"));

    DatabaseEnvironment backupEnvironment = DatabaseEnvironments.environments.get(backupEnv);
    DatabaseEnvironment restoreEnvironment = DatabaseEnvironments.environments.get(restoreEnv);

    MediaMogulRestoreExecutor mediaMogulRestoreExecutor = new MediaMogulRestoreExecutor(backupEnvironment, restoreEnvironment, oldBackup);
    mediaMogulRestoreExecutor.runUpdate();
  }

  public MediaMogulRestoreExecutor(DatabaseEnvironment backupEnvironment, DatabaseEnvironment restoreEnvironment, boolean oldBackup) {
    this.backupEnvironment = backupEnvironment;
    this.restoreEnvironment = restoreEnvironment;
    this.oldBackup = oldBackup;
  }

  public void runUpdate() throws InterruptedException, IOException, com.mayhew3.postgresobject.exception.MissingEnvException {
    if (restoreEnvironment.isLocal()) {
      updateLocal();
    } else {
      updateRemote();
    }
  }

  private void updateLocal() throws MissingEnvException, InterruptedException, IOException {
    LocalDatabaseEnvironment localRestoreEnvironment = (LocalDatabaseEnvironment) restoreEnvironment;

    DataRestoreExecutor dataRestoreExecutor;
    if (oldBackup) {
      dataRestoreExecutor = new DataRestoreLocalExecutor(localRestoreEnvironment, backupEnvironment, GlobalConstants.appLabel, backupDate);
    } else {
      dataRestoreExecutor = new DataRestoreLocalExecutor(localRestoreEnvironment, backupEnvironment, GlobalConstants.appLabel);
    }
    dataRestoreExecutor.runUpdate();

  }

  private void updateRemote() throws MissingEnvException, IOException, InterruptedException {
    HerokuDatabaseEnvironment herokuRestoreEnvironment = (HerokuDatabaseEnvironment) restoreEnvironment;

    DataRestoreExecutor dataRestoreExecutor;
    if (oldBackup) {
      dataRestoreExecutor = new DataRestoreRemoteExecutor(herokuRestoreEnvironment, backupEnvironment, GlobalConstants.appLabel, backupDate);
    } else {
      dataRestoreExecutor = new DataRestoreRemoteExecutor(herokuRestoreEnvironment, backupEnvironment, GlobalConstants.appLabel);
    }
    dataRestoreExecutor.runUpdate();
  }

}
