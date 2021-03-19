package com.mayhew3.mediamogul.games;

import com.mayhew3.mediamogul.model.games.GameLog;
import com.mayhew3.mediamogul.model.games.GameplaySession;
import com.mayhew3.mediamogul.scheduler.UpdateRunner;
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
import org.joda.time.DateTime;

import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class SteamPlaySessionGenerator implements UpdateRunner {

  private SQLConnection connection;
  private Integer person_id;

  private static Logger logger = LogManager.getLogger(SteamPlaySessionGenerator.class);

  public SteamPlaySessionGenerator(SQLConnection connection, Integer person_id) {
    this.connection = connection;
    this.person_id = person_id;
  }

  public static void main(String[] args) throws SQLException, URISyntaxException, MissingEnvException {
    ArgumentChecker argumentChecker = new ArgumentChecker(args);

    SQLConnection connection = PostgresConnectionFactory.createConnection(argumentChecker);

    String personIDString = EnvironmentChecker.getOrThrow("MediaMogulPersonID");
    assert personIDString != null;

    Integer person_id = Integer.parseInt(personIDString);

    SteamPlaySessionGenerator updateRunner = new SteamPlaySessionGenerator(connection, person_id);
    updateRunner.runUpdate();
  }


  @Override
  public void runUpdate() throws SQLException {
    Integer numberOfGames = getNumberOfGames();

    logger.info("Found " + numberOfGames + " newly played Steam games. Grouping into sessions.");

    String sql = "SELECT DISTINCT game_id " +
        getNewlyPlayedGameQuery() +
        "ORDER BY game_id ";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, "Played");

    Integer i = 0;
    while (resultSet.next()) {
      i++;
      int gameID = resultSet.getInt("game_id");
      debug("Processing game " + i + " of " + numberOfGames + ": id " + gameID);
      updateSessions(gameID);
      debug("Finished game " + i + " of " + numberOfGames + ".");
    }
  }

  private Integer getNumberOfGames() throws SQLException {
    String sql = "SELECT COUNT(DISTINCT game_id) as game_count " +
        getNewlyPlayedGameQuery();

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, "Played");
    if (resultSet.next()) {
      return resultSet.getInt("game_count");
    }

    throw new RuntimeException("No returned value for my game count query.");
  }

  @NotNull
  private String getNewlyPlayedGameQuery() {
    return "FROM game_log " +
        "WHERE eventtype = ? " +
        "AND gameplay_session_id IS NULL ";
  }

  private void updateSessions(Integer gameID) throws SQLException {
    Integer numberOfUnprocessedGameLogs = getNumberOfUnprocessedGameLogs(gameID);

    debug("Found " + numberOfUnprocessedGameLogs + " of new logs for game id " + gameID);

    String sql = "SELECT * " +
        getUnprocessedGameLogQuery() +
        "ORDER BY eventdate ";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, gameID, "Played");

    GameLog previousGameLog = null;
    List<GameLog> connectedLogs = new ArrayList<>();
    GameplaySession sessionToContinue = null;

    Integer i = 0;
    while (resultSet.next()) {
      i++;

      GameLog gameLog = new GameLog();
      gameLog.initializeFromDBObject(resultSet);

      debug("  - (" + i + "/" + numberOfUnprocessedGameLogs + "): " + gameLog.eventdate.getValue());

      if (previousGameLog == null) {
        // check if we can append this to an existing session.
        sessionToContinue = findSessionToContinue(gameLog);
      } else {
        if (areDifferentSessions(previousGameLog, gameLog)) {
          createOrUpdateGameplaySession(connectedLogs, sessionToContinue);
          sessionToContinue = null;
          connectedLogs = new ArrayList<>();
        }
      }

      connectedLogs.add(gameLog);
      previousGameLog = gameLog;
    }

    if (previousGameLog != null) {
      createOrUpdateGameplaySession(connectedLogs, sessionToContinue);
    }
  }

  private Integer getNumberOfUnprocessedGameLogs(Integer gameID) throws SQLException {
    String sql = "SELECT COUNT(1) as log_count " + getUnprocessedGameLogQuery();

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, gameID, "Played");
    if (resultSet.next()) {
      return resultSet.getInt("log_count");
    }

    throw new RuntimeException("No returned value for my log count query.");
  }

  @NotNull
  private String getUnprocessedGameLogQuery() {
    return "FROM game_log " +
        "WHERE game_id = ? " +
        "AND eventtype = ? " +
        "AND gameplay_session_id IS NULL ";
  }

  private static void debug(Object message) {
    logger.debug(message);
  }

  private void createOrUpdateGameplaySession(List<GameLog> connectedLogs, GameplaySession sessionToContinue) throws SQLException {
    debug(" End of session found. Grouping " + connectedLogs.size() + " logs into session.");
    if (sessionToContinue == null) {
      debug(" Creating new session.");
      createGameplaySession(connectedLogs);
    } else {
      debug(" Appending to existing session: " + sessionToContinue.startTime.getValue());
      appendToGameplaySession(sessionToContinue, connectedLogs);
    }
  }

  private void appendToGameplaySession(@NotNull GameplaySession gameplaySession, @NotNull List<GameLog> connectedLogs) throws SQLException {
    Integer initialMinutes = getInitialMinutes(gameplaySession);
    if (initialMinutes == null) {
      throw new IllegalStateException("No previousplaytime found with gameplay_session_id " + gameplaySession.id.getValue());
    }

    GameLog lastLog = getLastLog(connectedLogs);

    Integer totalMinutes = lastLog.updatedplaytime.getValue().intValue() - initialMinutes;

    gameplaySession.minutes.changeValue(totalMinutes);
    gameplaySession.commit(connection);

    markGameLogsUsed(connectedLogs, gameplaySession);
  }

  private @Nullable Integer getInitialMinutes(@NotNull GameplaySession gameplaySession) throws SQLException {
    String sql = "SELECT MIN(previousplaytime) AS initial_playtime " +
        "FROM game_log " +
        "WHERE gameplay_session_id = ? ";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, gameplaySession.id.getValue());
    if (resultSet.next()) {
      return resultSet.getBigDecimal("initial_playtime").intValue();
    } else {
      return null;
    }
  }

  private @Nullable GameplaySession findSessionToContinue(@NotNull GameLog gameLog) throws SQLException {
    DateTime finishTime = new DateTime(gameLog.eventdate.getValue());
    Integer diff = gameLog.diff.getValue().intValue();

    DateTime threshold = finishTime.minusMinutes(diff + 30);

    String sql = "SELECT gameplay_session_id, MAX(eventdate) " +
        "FROM game_log " +
        "WHERE game_id = ? " +
        "AND eventdate > ? " +
        "AND gameplay_session_id IS NOT NULL " +
        "GROUP BY gameplay_session_id " +
        "ORDER BY MAX(eventdate) DESC ";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, gameLog.gameID.getValue(),
        new Timestamp(threshold.toDate().getTime()));

    if (resultSet.next()) {
      Integer gameplaySessionId = resultSet.getInt("gameplay_session_id");
      GameplaySession foundSession = getGameplaySessionWithID(gameplaySessionId);

      if (foundSession == null) {
        throw new IllegalStateException("game_log found with gameplay_session_id " + gameplaySessionId + " that doesn't correspond to a gameplay_session.id.");
      }

      return foundSession;
    }

    return null;
  }

  private @Nullable GameplaySession getGameplaySessionWithID(@NotNull Integer id) throws SQLException {
    String sql = "SELECT * " +
        "FROM gameplay_session " +
        "WHERE id = ? ";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, id);
    if (resultSet.next()) {
      GameplaySession gameplaySession = new GameplaySession();
      gameplaySession.initializeFromDBObject(resultSet);
      return gameplaySession;
    } else {
      return null;
    }
  }

  private void createGameplaySession(@NotNull List<GameLog> connectedLogs) throws SQLException {
    GameplaySession gameplaySession = new GameplaySession();
    gameplaySession.initializeForInsert();

    gameplaySession.gameID.changeValue(getFirstLog(connectedLogs).gameID.getValue());
    gameplaySession.startTime.changeValue(getStartTime(connectedLogs));
    gameplaySession.minutes.changeValue(getTotalMinutes(connectedLogs));
    gameplaySession.manualAdjustment.changeValue(0);
    gameplaySession.person_id.changeValue(person_id);

    gameplaySession.commit(connection);

    markGameLogsUsed(connectedLogs, gameplaySession);
  }

  private Timestamp getStartTime(@NotNull List<GameLog> connectedLogs) {
    GameLog firstLog = connectedLogs.get(0);

    DateTime eventDate = new DateTime(firstLog.eventdate.getValue());
    Integer diff = firstLog.diff.getValue().intValue();

    return new Timestamp(eventDate.minusMinutes(diff).toDate().getTime());
  }

  private Integer getTotalMinutes(List<GameLog> connectedLogs) {
    GameLog firstLog = getFirstLog(connectedLogs);
    GameLog lastLog = getLastLog(connectedLogs);

    Integer initialMinutes = firstLog.previousPlaytime.getValue().intValue();
    Integer finalMinutes = lastLog.updatedplaytime.getValue().intValue();

    return finalMinutes - initialMinutes;
  }

  private GameLog getFirstLog(@NotNull List<GameLog> connectedLogs) {
    return connectedLogs.get(0);
  }

  private GameLog getLastLog(@NotNull List<GameLog> connectedLogs) {
    return connectedLogs.get(connectedLogs.size() - 1);
  }


  private void markGameLogsUsed(@NotNull List<GameLog> connectedLogs, @NotNull GameplaySession gameplaySession) throws SQLException {
    for (GameLog gameLog : connectedLogs) {
      gameLog.gameplaySessionID.changeValue(gameplaySession.id.getValue());
      gameLog.commit(connection);
    }
  }

  private Boolean areDifferentSessions(@NotNull GameLog olderGameLog, @NotNull GameLog newerGameLog) {
    Integer diff = newerGameLog.diff.getValue().intValue();
    Integer expandedDiff = diff + 30;

    DateTime newerGameTime = new DateTime(newerGameLog.eventdate.getValue().getTime());
    DateTime olderGameTime = new DateTime(olderGameLog.eventdate.getValue().getTime());

    return newerGameTime.minusMinutes(expandedDiff).isAfter(olderGameTime);
  }

  @Override
  public String getRunnerName() {
    return "Steam Play Session Generator";
  }

  @Override
  public @Nullable UpdateMode getUpdateMode() {
    return null;
  }

}
