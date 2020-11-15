package com.mayhew3.mediamogul.games;

import com.google.common.collect.Lists;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.mayhew3.mediamogul.exception.MissingEnvException;
import com.mayhew3.mediamogul.games.provider.IGDBProvider;
import com.mayhew3.mediamogul.games.provider.IGDBProviderImpl;
import com.mayhew3.mediamogul.model.games.Game;
import com.mayhew3.mediamogul.scheduler.UpdateRunner;
import com.mayhew3.mediamogul.tv.helper.UpdateMode;
import com.mayhew3.mediamogul.xml.JSONReader;
import com.mayhew3.mediamogul.xml.JSONReaderImpl;
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
import java.util.List;
import java.util.Map;

public class IGDBUpdateRunner implements UpdateRunner {

  private final SQLConnection connection;
  private final IGDBProvider igdbProvider;
  private final JSONReader jsonReader;
  private final UpdateMode updateMode;

  private final Map<UpdateMode, Runnable> methodMap;

  private static final Logger logger = LogManager.getLogger(IGDBUpdateRunner.class);

  public IGDBUpdateRunner(SQLConnection connection, IGDBProvider igdbProvider, JSONReader jsonReader, UpdateMode updateMode) {
    methodMap = new HashMap<>();
    methodMap.put(UpdateMode.SMART, this::runUpdateSmart);
    methodMap.put(UpdateMode.SINGLE, this::runUpdateSingle);
    methodMap.put(UpdateMode.SANITY, this::runUpdateSanity);
    methodMap.put(UpdateMode.FULL, this::runUpdateOnAll);
    methodMap.put(UpdateMode.MANUAL, this::runUpdateSplitter);

    this.connection = connection;
    this.igdbProvider = igdbProvider;
    this.jsonReader = jsonReader;

    if (!methodMap.containsKey(updateMode)) {
      throw new IllegalArgumentException("Update type '" + updateMode + "' is not applicable for this updater.");
    }

    this.updateMode = updateMode;
  }

  public static void main(String[] args) throws SQLException, URISyntaxException, MissingEnvException, UnirestException {
    ArgumentChecker argumentChecker = new ArgumentChecker(args);

    UpdateMode updateMode = UpdateMode.getUpdateModeOrDefault(argumentChecker, UpdateMode.FULL);

    SQLConnection connection = PostgresConnectionFactory.createConnection(argumentChecker);

    IGDBUpdateRunner igdbUpdateRunner = new IGDBUpdateRunner(connection, new IGDBProviderImpl(), new JSONReaderImpl(), updateMode);
    igdbUpdateRunner.runUpdate();
  }

  private static void debug(Object message) {
    logger.debug(message);
  }


  @Override
  public String getRunnerName() {
    return "IGDB Updater";
  }

  @Override
  public @Nullable UpdateMode getUpdateMode() {
    return null;
  }

  @Override
  public String getUniqueIdentifier() {
    return "igdb_updater";
  }

  @Override
  public void runUpdate() {

    try {
      methodMap.get(updateMode).run();
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  private void runUpdateSmart() {
    String sql = "SELECT * " +
        "FROM valid_game " +
        "WHERE igdb_success IS NULL " +
        "AND igdb_failed IS NULL " +
        "AND igdb_ignored IS NULL ";

    try {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql);
      runUpdateOnResultSet(resultSet);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void runUpdateSplitter() {
    List<Integer> gameIDs = Lists.newArrayList(578, 26);
    String sql = "SELECT * " +
        "FROM valid_game " +
        "WHERE id IN (?, ?) ";
    try {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, gameIDs.get(0), gameIDs.get(1), 0);

      runUpdateOnResultSet(resultSet);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void runUpdateSingle() {
    String gameTitle = "God of War";
    String sql = "select * " +
        "from valid_game " +
        "where title = ? ";
    try {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, gameTitle, 0);

      runUpdateOnResultSet(resultSet);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void runUpdateSanity() {
    String sql = "SELECT * " +
        "FROM valid_game " +
        "WHERE igdb_next_update < now() ";

    try {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql);

      runUpdateOnResultSet(resultSet);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void runUpdateOnAll() {
    String sql = "SELECT * " +
        "FROM valid_game " +
        "WHERE igdb_id IS NOT NULL " +
        "ORDER BY id DESC ";

    try {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql);

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
      Game game = new Game();

      try {
        processSingleGame(resultSet, game);
      } catch (Exception e) {
        logger.error("Show failed on initialization from DB.");
      }
    }

    debug("Update complete for result set: " + i + " processed.");
  }

  private void processSingleGame(ResultSet resultSet, Game game) throws SQLException {
    game.initializeFromDBObject(resultSet);

    try {
      updateIGDB(game);
    } catch (Exception e) {
      e.printStackTrace();
      logger.warn("Game failed IGDB: " + game.title.getValue() + " (ID " + game.id.getValue() + ")");
    }
  }

  private void updateIGDB(Game game) throws SQLException {
    IGDBUpdater updater = new IGDBUpdater(game, connection, igdbProvider, jsonReader);
    updater.updateGame();
  }

}
