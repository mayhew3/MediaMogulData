package com.mayhew3.mediamogul.tv;

import com.cloudinary.Cloudinary;
import com.cloudinary.Singleton;
import com.google.common.base.Preconditions;
import com.mayhew3.mediamogul.model.tv.Series;
import com.mayhew3.mediamogul.model.tv.TVDBPoster;
import com.mayhew3.mediamogul.scheduler.UpdateRunner;
import com.mayhew3.mediamogul.tv.exception.ShowFailedException;
import com.mayhew3.mediamogul.tv.helper.UpdateMode;
import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.EnvironmentChecker;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
import com.mayhew3.postgresobject.exception.MissingEnvException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class CloudinaryUploadRunner implements UpdateRunner {

  private Cloudinary cloudinary;
  private SQLConnection connection;

  private final Map<UpdateMode, Runnable> methodMap;

  private UpdateMode updateMode;

  private static Logger logger = LogManager.getLogger(CloudinaryUploadRunner.class);

  public CloudinaryUploadRunner(SQLConnection connection, @NotNull UpdateMode updateMode) throws MissingEnvException {
    EnvironmentChecker.getOrThrow("CLOUDINARY_URL");

    this.connection = connection;
    this.cloudinary = Singleton.getCloudinary();

    Preconditions.checkState("media-mogul".equals(this.cloudinary.config.cloudName),
        "CLOUDINARY_URL environment variable should initialize Cloudinary singleton with media-mogul cloud name.");

    methodMap = new HashMap<>();
    methodMap.put(UpdateMode.SINGLE, this::runUpdateSingle);
    methodMap.put(UpdateMode.FULL, this::runFullUpdate);

    if (!methodMap.containsKey(updateMode)) {
      throw new IllegalArgumentException("Update type '" + updateMode + "' is not applicable for this updater.");
    }

    this.updateMode = updateMode;
  }

  public static void main(String... args) throws URISyntaxException, SQLException, MissingEnvException {
    ArgumentChecker argumentChecker = new ArgumentChecker(args);
    UpdateMode updateMode = UpdateMode.getUpdateModeOrDefault(argumentChecker, UpdateMode.QUICK);

    SQLConnection connection = PostgresConnectionFactory.createConnection(argumentChecker);
    CloudinaryUploadRunner cloudinaryUploadRunner = new CloudinaryUploadRunner(connection, updateMode);
    cloudinaryUploadRunner.runUpdate();
  }

  @Override
  public void runUpdate() {
    try {
      methodMap.get(updateMode).run();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void runUpdateSingle() {
    String sql = "SELECT * " +
            "FROM series " +
            "WHERE title = ? ";

    try {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, "Mad Men");
      runUpdateOnResultSet(resultSet);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void runFullUpdate() {
    runUpdateOnAllSeries();
  }

  private void runUpdateOnAllSeries() {
    logger.info("Running full version on all shows.");

    String sql = "SELECT s.* " +
            "FROM series s " +
            "WHERE s.retired = ? ";

    try {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, 0);
      runUpdateOnResultSet(resultSet);
    } catch (SQLException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  private void runUpdateOnAllUnmatchedPosters() {
    logger.info("Running on unmatched posters.");

    String sql = "SELECT * " +
        "FROM tvdb_poster " +
        "WHERE cloud_poster IS NULL " +
        "AND hidden IS NULL " +
        "AND retired = ? ";

    try {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, 0);
      runUpdateOnPosterResultSet(resultSet);
    } catch (SQLException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  private void runUpdateOnResultSet(ResultSet resultSet) throws SQLException {
    int postersProcessed = 0;
    int postersFailed = 0;

    while (resultSet.next()) {
      postersProcessed++;

      logger.debug("Processing poster " + postersProcessed + "... (" + postersFailed + " failed so far.)");

      Series series = new Series();
      series.initializeFromDBObject(resultSet);

      CloudinaryUpdater cloudinaryUpdater = new CloudinaryUpdater(cloudinary, series, connection);
      try {
        cloudinaryUpdater.updateSeries();
      } catch (ShowFailedException e) {
        postersFailed++;
      }
    }

    logger.info("Finished processing " + postersProcessed + " posters: " +
        (postersProcessed - postersFailed) + " were updated successfully.");
  }

  private void runUpdateOnPosterResultSet(ResultSet resultSet) throws SQLException {
    int postersProcessed = 0;
    int postersFailed = 0;

    while (resultSet.next()) {
      postersProcessed++;

      logger.debug("Processing poster " + postersProcessed + "...");

      TVDBPoster poster = new TVDBPoster();
      poster.initializeFromDBObject(resultSet);

      CloudinaryUpdater cloudinaryUpdater = new CloudinaryUpdater(cloudinary, poster, connection);
      try {
        cloudinaryUpdater.updateTVDBPoster();
      } catch (ShowFailedException e) {
        postersFailed++;
      }
    }

    logger.info("Finished processing " + postersProcessed + " posters: " +
        (postersProcessed - postersFailed) + " were updated successfully.");
  }

  @Override
  public String getRunnerName() {
    return "Cloudinary Updater";
  }

  @Override
  public @Nullable UpdateMode getUpdateMode() {
    return updateMode;
  }

}
