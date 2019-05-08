package com.mayhew3.mediamogul.tv;

import com.mayhew3.mediamogul.model.tv.Series;
import com.mayhew3.mediamogul.scheduler.UpdateRunner;
import com.mayhew3.mediamogul.tv.helper.MetacriticException;
import com.mayhew3.mediamogul.tv.helper.UpdateMode;
import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class MetacriticTVUpdateRunner implements UpdateRunner {

  private SQLConnection connection;
  private UpdateMode updateMode;

  private final Map<UpdateMode, Runnable> methodMap;

  private static Logger logger = LogManager.getLogger(MetacriticTVUpdateRunner.class);

  public MetacriticTVUpdateRunner(SQLConnection connection, UpdateMode updateMode) {
    methodMap = new HashMap<>();
    methodMap.put(UpdateMode.FULL, this::runFullUpdate);
    methodMap.put(UpdateMode.SINGLE, this::runUpdateSingle);
    methodMap.put(UpdateMode.CHUNKED, this::runUpdateChunked);
    methodMap.put(UpdateMode.SANITY, this::runUpdateChunkedSanity);

    this.connection = connection;

    if (!methodMap.keySet().contains(updateMode)) {
      throw new IllegalArgumentException("Update mode '" + updateMode + "' is not applicable for this updater.");
    }

    this.updateMode = updateMode;
  }

  public static void main(String... args) throws URISyntaxException, SQLException {
    ArgumentChecker argumentChecker = new ArgumentChecker(args);
    UpdateMode updateMode = UpdateMode.getUpdateModeOrDefault(argumentChecker, UpdateMode.FULL);

    SQLConnection connection = PostgresConnectionFactory.createConnection(argumentChecker);
    MetacriticTVUpdateRunner metacriticTVUpdateRunner = new MetacriticTVUpdateRunner(connection, updateMode);

    metacriticTVUpdateRunner.runUpdate();
  }

  @Override
  public void runUpdate() {
    methodMap.get(updateMode).run();
  }

  void runFullUpdate() {
    String sql = "select * " +
        "from series " +
        "where tvdb_match_status = ? " +
        "and retired = ? ";

    try {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, TVDBMatchStatus.MATCH_COMPLETED, 0);
      runUpdateOnResultSet(resultSet);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void runUpdateOnMatched() {
    String sql = "select * " +
        "from series " +
        "where tvdb_match_status = ? " +
        "and metacritic IS NULL " +
        "and metacritic_confirmed IS NOT NULL " +
        "and retired = ? ";

    try {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, TVDBMatchStatus.MATCH_COMPLETED, 0);
      runUpdateOnResultSet(resultSet);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void runUpdateChunked() {
    String sql = "select * " +
        "from series " +
        "where tvdb_match_status = ? " +
        "and metacritic IS NULL " +
        "and retired = ? " +
        "ORDER BY metacritic_failed NULLS FIRST, id " +
        "LIMIT 8 ";

    try {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, TVDBMatchStatus.MATCH_COMPLETED, 0);
      runUpdateOnResultSet(resultSet);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void runUpdateChunkedSanity() {
    String sql = "select * " +
        "from series " +
        "where tvdb_match_status = ? " +
        "and metacritic IS NOT NULL " +
        "and retired = ? " +
        "ORDER BY metacritic_success NULLS FIRST, id " +
        "LIMIT 8 ";

    try {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, TVDBMatchStatus.MATCH_COMPLETED, 0);
      runUpdateOnResultSet(resultSet);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void runUpdateSingle() {
    String singleSeriesTitle = "Battlestar Galactica (2003)"; // update for testing on a single series

    String sql = "select * " +
        "from series " +
        "where tvdb_match_status = ? " +
        "and title = ? " +
        "and retired = ? ";

    try {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, TVDBMatchStatus.MATCH_COMPLETED, singleSeriesTitle, 0);
      runUpdateOnResultSet(resultSet);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void runUpdateOnResultSet(ResultSet resultSet) throws SQLException {
    debug("Starting update.");

    int i = 0;

    while (resultSet.next()) {
      i++;
      Series series = new Series();
      series.initializeFromDBObject(resultSet);

      try {
        MetacriticTVUpdater metacriticTVUpdater = new MetacriticTVUpdater(series, connection);
        metacriticTVUpdater.parseMetacritic();
        series.metacriticNew.changeValue(false);
        series.commit(connection);
      } catch (MetacriticException e) {
        logger.warn("Unable to find metacritic for: " + series.seriesTitle.getValue());
        series.metacriticNew.changeValue(false);
        series.commit(connection);
      } catch (Exception e) {
        e.printStackTrace();
        logger.error("Uncaught exception during metacritic fetch: " + series.seriesTitle.getValue());
      }

      debug(i + " processed.");
    }
    if (i == 0) {
      logger.info("No rows found to process. Finished.");
    } else {
      logger.info("Finished processing " + i + " rows.");
    }
  }

  private void debug(Object message) {
    logger.debug(message);
  }

  @Override
  public String getRunnerName() {
    return "Metacritic TV Runner";
  }

  @Override
  public @Nullable UpdateMode getUpdateMode() {
    return updateMode;
  }

}
