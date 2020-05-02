package com.mayhew3.mediamogul.games;

import com.google.common.collect.Lists;
import com.mayhew3.mediamogul.EnvironmentChecker;
import com.mayhew3.mediamogul.exception.MissingEnvException;
import com.mayhew3.mediamogul.exception.SingleFailedException;
import com.mayhew3.mediamogul.games.exception.MetacriticElementNotFoundException;
import com.mayhew3.mediamogul.games.exception.MetacriticPageNotFoundException;
import com.mayhew3.mediamogul.games.exception.MetacriticPlatformNameException;
import com.mayhew3.mediamogul.model.games.AvailableGamePlatform;
import com.mayhew3.mediamogul.model.games.Game;
import com.mayhew3.mediamogul.scheduler.UpdateRunner;
import com.mayhew3.mediamogul.tv.helper.UpdateMode;
import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MetacriticGameUpdateRunner implements UpdateRunner {

  private final UpdateMode updateMode;
  private final Integer person_id;

  public enum MetacriticUpdateResult {PAGE_NOT_FOUND, ELEMENT_NOT_FOUND, SUCCESS}

  private final Map<UpdateMode, Runnable> methodMap;

  private final SQLConnection connection;

  private static final Logger logger = LogManager.getLogger(MetacriticGameUpdateRunner.class);

  public MetacriticGameUpdateRunner(SQLConnection connection, UpdateMode updateMode, Integer person_id) {
    this.person_id = person_id;
    methodMap = new HashMap<>();
    methodMap.put(UpdateMode.FULL, this::updateAllGames);
    methodMap.put(UpdateMode.UNMATCHED, this::updateUnmatchedGames);
    methodMap.put(UpdateMode.OLD_ERRORS, this::updateMatchGamesWithNoValue);
    methodMap.put(UpdateMode.SINGLE, this::updateSingleGame);
    methodMap.put(UpdateMode.SERVICE, this::updatePlatformGames);
    methodMap.put(UpdateMode.SMART, this::updateSmartGames);

    this.connection = connection;

    if (!methodMap.containsKey(updateMode)) {
      throw new IllegalArgumentException("Update mode '" + updateMode + "' is not applicable for this updater.");
    }

    this.updateMode = updateMode;
  }

  public static void main(String[] args) throws FileNotFoundException, SQLException, URISyntaxException, MissingEnvException {
    List<String> argList = Lists.newArrayList(args);
    boolean logToFile = argList.contains("LogToFile");
    ArgumentChecker argumentChecker = new ArgumentChecker(args);
    UpdateMode updateMode = UpdateMode.getUpdateModeOrDefault(argumentChecker, UpdateMode.UNMATCHED);

    if (logToFile) {
      String mediaMogulLogs = EnvironmentChecker.getOrThrow("MediaMogulLogs");

      File file = new File(mediaMogulLogs + "\\MetacriticGameUpdater.log");
      FileOutputStream fos = new FileOutputStream(file, true);
      PrintStream ps = new PrintStream(fos);
      System.setErr(ps);
      System.setOut(ps);
    }

    SQLConnection connection = PostgresConnectionFactory.createConnection(argumentChecker);

    String mediaMogulPersonID = EnvironmentChecker.getOrThrow("MediaMogulPersonID");
    Integer person_id = Integer.parseInt(mediaMogulPersonID);

    MetacriticGameUpdateRunner updateRunner = new MetacriticGameUpdateRunner(connection, updateMode, person_id);
    updateRunner.runUpdate();
  }

  @Override
  public String getRunnerName() {
    return "Metacritic Game Updater";
  }

  @Override
  public @Nullable UpdateMode getUpdateMode() {
    return updateMode;
  }

  public void runUpdate() {
    methodMap.get(updateMode).run();
  }

  private void updateSingleGame() {
    String nameOfSingleGame = "Mario + Rabbids Kingdom Battle";

    String sql = "SELECT * " +
        "FROM valid_game " +
        "WHERE title = ? ";

    try {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, nameOfSingleGame);

      runUpdateOnGameResultSet(resultSet);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void updateAllGames() {
    String sql = "SELECT * " +
        "FROM valid_game";

    try {
      ResultSet resultSet = connection.executeQuery(sql);
      runUpdateOnGameResultSet(resultSet);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void updatePlatformGames() {
    String platformName = "Switch";
    String sql = "SELECT g.* " +
        "FROM valid_game g " +
        "INNER JOIN available_game_platform agp " +
        "  ON agp.game_id = g.id " +
        "WHERE agp.platform_name = ? " +
        "AND agp.metacritic_page = ? ";

    try {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, platformName, false);
      runUpdateOnGameResultSet(resultSet);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void updateUnmatchedGames() {
    String baseSql = "FROM valid_game g " +
        "INNER JOIN available_game_platform agp " +
        "  ON agp.game_id = g.id " +
        "WHERE agp.metacritic IS NULL ";
    String fieldsSql = "SELECT agp.* ";

    runBaseAndCountOnSQL(baseSql, fieldsSql);
  }

  private void updateSmartGames() {
    String baseSql = "FROM valid_game g " +
        "INNER JOIN available_game_platform agp " +
        "  ON agp.game_id = g.id " +
        "WHERE (agp.metacritic_next_update IS NULL OR agp.metacritic_next_update < ?) " +
        "AND agp.id < ? ";
    String sql = "SELECT agp.* ";
    Timestamp now = new Timestamp(new Date().getTime());
    runBaseAndCountOnSQL(baseSql, sql, now, 200);
  }

  private void updateMatchGamesWithNoValue() {
    String sql = "SELECT * " +
        "FROM valid_game " +
        "WHERE metacritic_page = ? " +
        "AND metacritic IS NULL ";

    try {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, true);

      runUpdateOnGameResultSet(resultSet);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void runUpdateOnGameResultSet(ResultSet resultSet) throws SQLException {
    int i = 1;
    int failures = 0;

    while (resultSet.next()) {
      Game game = new Game();
      try {
        game.initializeFromDBObject(resultSet);

        debug("Updating game: " + game.title.getValue());

        List<AvailableGamePlatform> availableGamePlatforms = game.getAvailableGamePlatforms(connection);
        for (AvailableGamePlatform platform : availableGamePlatforms) {
          MetacriticGameUpdater metacriticGameUpdater = new MetacriticGameUpdater(game, connection, person_id, platform);
          metacriticGameUpdater.runUpdater();
        }
      } catch (SingleFailedException e) {
        logger.warn(e.getMessage());
        logger.warn("Show failed: " + game.title.getValue());
        failures++;
      } catch (SQLException e) {
        e.printStackTrace();
        logger.error("Failed to load game from database.");
        failures++;
      }

      debug(i + " processed.");
      i++;
    }

    if (i > 1) {
      logger.info("Operation completed! Failed on " + failures + "/" + (i - 1) + " games (" + (100 * failures / (i - 1)) + "%)");
    }
  }

  private void runUpdateOnPlatformResultSet(ResultSet resultSet, int rowCount) throws SQLException {
    int pageFailures = 0;
    int elementFailures = 0;
    int successes = 0;
    int platformFailures = 0;
    int otherFailures = 0;

    int i = 1;

    while (resultSet.next()) {
      AvailableGamePlatform availableGamePlatform = new AvailableGamePlatform();
      try {
        availableGamePlatform.initializeFromDBObject(resultSet);

        Game game = availableGamePlatform.getGame(connection);

        debug("Updating game: " + game.title.getValue());

        MetacriticGameUpdater metacriticGameUpdater = new MetacriticGameUpdater(game, connection, person_id, availableGamePlatform);
        metacriticGameUpdater.runUpdater();
        successes++;
      } catch (MetacriticPageNotFoundException e) {
        pageFailures++;
        logger.debug(e.getLocalizedMessage());
      } catch (MetacriticPlatformNameException e) {
        platformFailures++;
        logger.debug(e.getLocalizedMessage());
      } catch (MetacriticElementNotFoundException e) {
        elementFailures++;
        logger.debug(e.getLocalizedMessage());
      } catch (Exception e) {
        otherFailures++;
        logger.info(e.getLocalizedMessage());
      }

      debug(i + " / " + rowCount + " processed.");
      i++;
    }

    if (i > 1) {
      int total = i - 1;
      logger.info("Operation completed!");
      logger.info(" - Successes: " + successes + " / " + total);
      logger.info(" - Page Not Found: " + pageFailures + " / " + total);
      logger.info(" - Element Not Found: " + elementFailures + " / " + total);
      logger.info(" - No Platform Name: " + platformFailures + " / " + total);
      logger.info(" - Other Failures: " + otherFailures + " / " + total);
    }
  }

  private void runBaseAndCountOnSQL(String baseSql, String fieldsSql, Object... params) {
    String countSql = "SELECT COUNT(1) as rowCount " + baseSql;
    String fullSql = fieldsSql + baseSql;

    try {
      int rowCount = getCount(countSql, params);
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(fullSql, params);

      runUpdateOnPlatformResultSet(resultSet, rowCount);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private int getCount(String countSql, Object... params) throws SQLException {
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(countSql, params);
    resultSet.next();
    return resultSet.getInt("rowCount");
  }


  private static void debug(Object message) {
    logger.debug(message);
  }
}

