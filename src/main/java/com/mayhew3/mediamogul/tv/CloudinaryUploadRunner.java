package com.mayhew3.mediamogul.tv;

import com.cloudinary.Cloudinary;
import com.cloudinary.Singleton;
import com.google.common.base.Preconditions;
import com.mayhew3.mediamogul.EnvironmentChecker;
import com.mayhew3.mediamogul.exception.MissingEnvException;
import com.mayhew3.mediamogul.model.tv.Series;
import com.mayhew3.mediamogul.scheduler.UpdateRunner;
import com.mayhew3.mediamogul.tv.helper.UpdateMode;
import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
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

  public CloudinaryUploadRunner(SQLConnection connection, @NotNull UpdateMode updateMode) throws MissingEnvException {
    EnvironmentChecker.getOrThrow("CLOUDINARY_URL");

    this.connection = connection;
    this.cloudinary = Singleton.getCloudinary();

    Preconditions.checkState("media-mogul".equals(this.cloudinary.config.cloudName),
        "CLOUDINARY_URL environment variable should initialize Cloudinary singleton with media-mogul cloud name.");

    methodMap = new HashMap<>();
    methodMap.put(UpdateMode.SINGLE, this::runUpdateSingle);
    methodMap.put(UpdateMode.FULL, this::runFullUpdate);

    if (!methodMap.keySet().contains(updateMode)) {
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
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, "You're the Worst");
      runUpdateOnResultSet(resultSet, true);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void runFullUpdate() {
    runUpdateOnTierOne();
    runUpdateOnAllNonSuggestionSeries();
  }

  private void runUpdateOnTierOne() {
    String sql = "SELECT s.* " +
            "FROM series s " +
            "INNER JOIN person_series ps " +
            "  ON ps.series_id = s.id " +
            "WHERE ps.tier = ? " +
            "AND ps.person_id = ? ";

    try {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, 1, 1);
      runUpdateOnResultSet(resultSet, true);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void runUpdateOnAllNonSuggestionSeries() {
    String sql = "SELECT s.* " +
            "FROM series s " +
            "WHERE s.suggestion = ? " +
            "AND s.retired = ? " +
            "AND s.poster IS NOT NULL " +
            "AND s.cloud_poster IS NULL ";

    try {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, false, 0);
      runUpdateOnResultSet(resultSet, false);
    } catch (SQLException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  private void runUpdateOnResultSet(ResultSet resultSet, Boolean otherPosters) throws SQLException {
    while (resultSet.next()) {
      Series series = new Series();
      series.initializeFromDBObject(resultSet);

      CloudinaryUpdater cloudinaryUpdater = new CloudinaryUpdater(cloudinary, series, connection);
      cloudinaryUpdater.updateSeries(otherPosters);
    }
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
