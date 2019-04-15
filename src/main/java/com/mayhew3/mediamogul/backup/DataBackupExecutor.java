package com.mayhew3.mediamogul.backup;

import com.google.common.collect.Lists;
import com.mayhew3.mediamogul.EnvironmentChecker;
import com.mayhew3.mediamogul.exception.MissingEnvException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class DataBackupExecutor {

  private static Logger logger = LogManager.getLogger(DataBackupExecutor.class);
  
  public static void main(String... args) throws IOException, InterruptedException, MissingEnvException {
    logger.info("Beginning execution of executor!");

    if (args.length == 0) {
      throw new IllegalArgumentException("Expect single argument with environment name to backup (heroku, heroku-staging, local).");
    }

    String env = args[0];
    List<String> possibleEnvs = Lists.newArrayList("heroku", "heroku-staging", "local");
    if (!possibleEnvs.contains(env)) {
      throw new IllegalArgumentException("Unexpected argument: '" + env +
          "'. Expected environment name to backup: (heroku, heroku-staging, local).");
    }

    logger.info("Backing up from environment '" + env + "'");

    String postgres_program_dir = EnvironmentChecker.getOrThrow("POSTGRES_PROGRAM_DIR");
    String backup_dir_location = EnvironmentChecker.getOrThrow("BACKUP_DIR_MM");

    String db_url;
    String postgres_pgpass;

    if ("heroku".equals(env)) {
      db_url = EnvironmentChecker.getOrThrow("postgresURL_heroku");
      postgres_pgpass = EnvironmentChecker.getOrThrow("postgres_pgpass_heroku");

    } else if ("heroku-staging".equals(env)) {
      db_url = EnvironmentChecker.getOrThrow("postgresURL_heroku_staging");
      postgres_pgpass = EnvironmentChecker.getOrThrow("postgres_pgpass_heroku");

    } else if ("local".equals(env)) {
      db_url = EnvironmentChecker.getOrThrow("postgresURL_local_backup");
      postgres_pgpass = EnvironmentChecker.getOrThrow("postgres_pgpass_local");

    } else {
      throw new IllegalArgumentException("Unexpected value for env.");
    }

    File pgpass_file = new File(postgres_pgpass);
    assert pgpass_file.exists() && pgpass_file.isFile();

    File postgres_program = new File(postgres_program_dir);
    assert postgres_program.exists() && postgres_program.isDirectory();

    File base_backup_dir = new File(backup_dir_location);
    assert base_backup_dir.exists() && base_backup_dir.isDirectory();

    File env_backup_dir = new File(backup_dir_location + "\\" + env);
    if (!env_backup_dir.exists()) {
      //noinspection ResultOfMethodCallIgnored
      env_backup_dir.mkdir();
    }


    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd_hh_mm_ss");
    String formattedDate = dateFormat.format(new Date());

    String fullBackupPath = backup_dir_location + "\\" + env + "\\" + formattedDate + ".dump";

    logger.info("Saving backup to file: " + fullBackupPath);

    ProcessBuilder processBuilder = new ProcessBuilder(
        postgres_program_dir + "\\pg_dump.exe",
        "--format=custom",
        "--verbose",
        "--file=" + fullBackupPath,
        db_url);
    processBuilder.environment().put("PGPASSFILE", postgres_pgpass);

    processBuilder.inheritIO();

    logger.info("Starting db backup process...");

    Process process = processBuilder.start();
    process.waitFor();

    logger.info("Finished db backup process!");

  }


}
