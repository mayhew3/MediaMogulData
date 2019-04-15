package com.mayhew3.mediamogul.backup;

import com.mayhew3.mediamogul.ArgumentChecker;
import com.mayhew3.mediamogul.EnvironmentChecker;
import com.mayhew3.mediamogul.exception.MissingEnvException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

public class DataRestoreExecutor {

  private static String aws_program_dir;
  private static String aws_user_dir;
  private static String heroku_program_dir;

  private static String postgres_program_dir;
  private static String postgres_pgpass_local;

  private static Logger logger = LogManager.getLogger(DataRestoreExecutor.class);

  private static Comparator<Path> created = (file1, file2) -> {
    try {
      BasicFileAttributes attr1 = Files.readAttributes(file1, BasicFileAttributes.class);
      BasicFileAttributes attr2 = Files.readAttributes(file2, BasicFileAttributes.class);

      return attr2.creationTime().compareTo(attr1.creationTime());
    } catch (IOException e) {
      e.printStackTrace();
    }
    return 0;
  };

  public static void main(String... args) throws IOException, InterruptedException, MissingEnvException {
    logger.info("Beginning execution of executor!");

    ArgumentChecker argumentChecker = new ArgumentChecker(args);
    argumentChecker.removeExpectedOption("db");
    argumentChecker.addExpectedOption("env", true, "Name of DB environment to which to restore database.");
    argumentChecker.addExpectedOption("backupEnv", true, "Name of DB environment whose backup should be restored.");
    argumentChecker.addExpectedOption("appName", false, "Name of Heroku app to which to restore database. Only required if env is Heroku.");

    String env = argumentChecker.getRequiredValue("env");
    String backupEnv = argumentChecker.getRequiredValue("backupEnv");
    Optional<String> optionalAppName = argumentChecker.getOptionalIdentifier("appName");

    aws_program_dir = EnvironmentChecker.getOrThrow("AWS_PROGRAM_DIR");
    aws_user_dir = EnvironmentChecker.getOrThrow("AWS_USER_DIR");
    heroku_program_dir = EnvironmentChecker.getOrThrow("HEROKU_PROGRAM_DIR");

    postgres_program_dir = EnvironmentChecker.getOrThrow("POSTGRES_PROGRAM_DIR");
    postgres_pgpass_local = EnvironmentChecker.getOrThrow("postgres_pgpass_local");
    String backup_dir_location = EnvironmentChecker.getOrThrow("BACKUP_DIR_MM");

    File pgpass_file = new File(postgres_pgpass_local);
    assert pgpass_file.exists() && pgpass_file.isFile();

    File postgres_program = new File(postgres_program_dir);
    assert postgres_program.exists() && postgres_program.isDirectory();

    File backup_dir = new File(backup_dir_location);
    assert backup_dir.exists() && backup_dir.isDirectory();

    File base_backup_dir = new File(backup_dir_location);
    assert base_backup_dir.exists() && base_backup_dir.isDirectory();

    File env_backup_dir = new File(backup_dir_location + "\\" + backupEnv);
    if (!env_backup_dir.exists()) {
      //noinspection ResultOfMethodCallIgnored
      env_backup_dir.mkdir();
    }

    Path latestBackup = getLatestBackup(env_backup_dir.getPath());
    logger.info("File to restore: " + latestBackup.toString());

    // If no appName, do local restore. Otherwise restore to app.
    if ("local".equalsIgnoreCase(env)) {
      logger.info("Restoring to local DB.");
      restoreToLocal(latestBackup, "tv");
    } else {
      if (optionalAppName.isPresent()) {
        String appName = optionalAppName.get();
        logger.info("Restoring to Heroku app '" + optionalAppName + "'");
        String outputPath = getAWSPath(latestBackup);
        copyDBtoAWS(latestBackup, outputPath);
        String result = getSignedUrl(outputPath);
        restoreToHeroku(appName, result);
      } else {
        throw new IllegalStateException("No appName found for env: " + env);
      }
    }
  }

  @SuppressWarnings("SameParameterValue")
  private static void restoreToLocal(Path latestBackup, String local_db_name) throws IOException, InterruptedException {

    ProcessBuilder processBuilder = new ProcessBuilder(
        postgres_program_dir + "\\pg_restore.exe",
        "--host=localhost",
        "--dbname=" + local_db_name,
        "--username=postgres",
        "--clean",
        "--format=custom",
        "--verbose",
        latestBackup.toString());
    processBuilder.environment().put("PGPASSFILE", postgres_pgpass_local);

    processBuilder.inheritIO();

    logger.info("Starting db restore process...");

    Process process = processBuilder.start();
    process.waitFor();

    logger.info("Finished db restore process!");

  }

  private static void restoreToHeroku(String appName, String result) throws IOException, InterruptedException {

    ProcessBuilder processBuilder = new ProcessBuilder(
        heroku_program_dir + "\\heroku.cmd",
        "pg:backups:restore",
        "--app=" + appName,
        "\"" + result + "\"",
        "--confirm=" + appName,
        "DATABASE_URL"
    );

    processBuilder.inheritIO();

    logger.info("Starting db restore process...");

    Process process = processBuilder.start();
    process.waitFor();

    logger.info("Finished db restore process!");
  }

  private static void copyDBtoAWS(Path latestBackup, String outputPath) throws IOException, InterruptedException {
    ProcessBuilder processBuilder = new ProcessBuilder(
        aws_program_dir + "\\aws.exe",
        "s3",
        "cp",
        latestBackup.toString(),
        outputPath);

    processBuilder.inheritIO();

    logger.info("Starting db copy to aws...");

    Process process = processBuilder.start();
    process.waitFor();

    logger.info("Finished db copy process!");
  }

  @NotNull
  private static String getAWSPath(Path latestBackup) {
    File aws_credentials = new File(aws_user_dir + "/credentials");
    assert aws_credentials.exists() && aws_credentials.isFile() :
        "Need to configure aws. See aws_info.txt";

    String bucketName = "s3://mediamogulbackups";
    return bucketName + "/" + latestBackup.getFileName();
  }

  @NotNull
  private static String getSignedUrl(String outputPath) throws IOException {

    ProcessBuilder processBuilder = new ProcessBuilder(
        aws_program_dir + "\\aws.exe",
        "s3",
        "presign",
        outputPath
    );

    logger.info("Starting aws signed url generation...");

    Process process = processBuilder.start();

    BufferedReader reader =
        new BufferedReader(new InputStreamReader(process.getInputStream()));
    StringBuilder builder = new StringBuilder();
    String line;
    while ( (line = reader.readLine()) != null) {
      builder.append(line);
    }
    String result = builder.toString();

    assert !"".equals(result) : "No text found in AWS signed url!";

    logger.info("Finished aws signed url generation: ");
    logger.info("URL: " + result);
    return result;
  }

  private static Path getLatestBackup(String backup_directory) throws IOException {
    Path path = Paths.get(backup_directory);
    List<Path> files = new ArrayList<>();
    DirectoryStream<Path> paths = Files.newDirectoryStream(path);
    for (Path path1 : paths) {
      File file = new File(path1.toString());
      if (file.isFile()) {
        files.add(path1);
      }
    }
    files.sort(created);
    return files.get(0);
  }


}
