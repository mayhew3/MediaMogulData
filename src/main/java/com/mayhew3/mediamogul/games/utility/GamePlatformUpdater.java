package com.mayhew3.mediamogul.games.utility;

import com.mashape.unirest.http.exceptions.UnirestException;
import com.mayhew3.mediamogul.db.DatabaseEnvironments;
import com.mayhew3.mediamogul.games.provider.IGDBProviderImpl;
import com.mayhew3.mediamogul.model.games.*;
import com.mayhew3.mediamogul.xml.JSONReader;
import com.mayhew3.mediamogul.xml.JSONReaderImpl;
import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.db.DatabaseEnvironment;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
import com.mayhew3.postgresobject.exception.MissingEnvException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.json.JSONArray;
import org.json.JSONObject;

import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class GamePlatformUpdater {
  private final SQLConnection connection;

  private static final Logger logger = LogManager.getLogger(GamePlatformUpdater.class);

  private final List<GamePlatform> allPlatforms = new ArrayList<>();
  private final JSONReader jsonReader = new JSONReaderImpl();

  private GamePlatformUpdater(SQLConnection connection) {
    this.connection = connection;
  }

  public static void main(String[] args) throws URISyntaxException, SQLException, MissingEnvException, UnirestException {
    ArgumentChecker argumentChecker = new ArgumentChecker(args);

    DatabaseEnvironment environment = DatabaseEnvironments.getEnvironmentForDBArgument(argumentChecker);
    SQLConnection connection = PostgresConnectionFactory.createConnection(environment);

    GamePlatformUpdater updater = new GamePlatformUpdater(connection);
    updater.runUpdate();
  }

  private void runUpdate() throws SQLException, MissingEnvException, UnirestException {
    populateAllPlatforms();

    String sql = "SELECT igdb_id " +
        "FROM valid_game " +
        "WHERE igdb_success IS NOT NULL " +
        "AND id NOT IN (SELECT game_id from available_game_platform) " +
        "GROUP BY igdb_id ";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql);
    while (resultSet.next()) {
      Integer igdb_id = resultSet.getInt("igdb_id");
      handleGame(igdb_id);
    }
  }

  private void populateAllPlatforms() throws SQLException, MissingEnvException, UnirestException {
    IGDBProviderImpl igdbProvider = new IGDBProviderImpl();
    JSONArray igdbPlatforms = igdbProvider.getAllPlatforms();

    String sql = "SELECT * " +
        "FROM game_platform ";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql);

    while (resultSet.next()) {
      GamePlatform gamePlatform = new GamePlatform();
      gamePlatform.initializeFromDBObject(resultSet);
      this.allPlatforms.add(gamePlatform);
    }

    List<String> unregisteredPlatforms = getUnregisteredPlatforms();
    for (String unregisteredPlatform : unregisteredPlatforms) {
      createPlatformFromIGDB(igdbPlatforms, unregisteredPlatform);
    }
  }

  private void createPlatformFromIGDB(JSONArray igdbPlatforms, String unregisteredPlatform) throws SQLException {
    GamePlatform gamePlatform = new GamePlatform();
    gamePlatform.initializeForInsert();
    gamePlatform.fullName.changeValue(unregisteredPlatform);
    gamePlatform.shortName.changeValue(unregisteredPlatform);

    Optional<JSONObject> maybeIGDBPlatform = findIGDBPlatformFromAbbreviation(igdbPlatforms, unregisteredPlatform);
    if (maybeIGDBPlatform.isPresent()) {
      JSONObject igdbPlatform = maybeIGDBPlatform.get();
      String igdbName = igdbPlatform.getString("name");
      Integer igdbID = igdbPlatform.getInt("id");

      gamePlatform.igdbName.changeValue(igdbName);
      gamePlatform.igdbPlatformId.changeValue(igdbID);
    }

    gamePlatform.commit(connection);
    allPlatforms.add(gamePlatform);
  }

  private List<String> getUnregisteredPlatforms() throws SQLException {
    String sql = "SELECT DISTINCT g.platform " +
        "FROM valid_game g " +
        "WHERE g.platform NOT IN (SELECT full_name FROM game_platform) ";
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql);
    List<String> platforms = new ArrayList<>();
    while(resultSet.next()) {
      String platform = resultSet.getString("platform");
      platforms.add(platform);
    }
    return platforms;
  }

  private Optional<JSONObject> findIGDBPlatformFromAbbreviation(JSONArray igdbPlatforms, String abbreviation) {
    // don't want to match to SteamOS.
    if (abbreviation.equalsIgnoreCase("Steam")) {
      return Optional.empty();
    }
    for (Object igdbPlatformObj : igdbPlatforms) {
      JSONObject igdbPlatform = (JSONObject)igdbPlatformObj;
      String igdbAbbr = jsonReader.getNullableStringWithKey(igdbPlatform,"abbreviation");
      String igdbName = jsonReader.getNullableStringWithKey(igdbPlatform,"name");
      if (abbreviation.equalsIgnoreCase(igdbAbbr) || abbreviation.equalsIgnoreCase(igdbName)) {
        return Optional.of(igdbPlatform);
      }
    }
    return Optional.empty();
  }

  private GamePlatform getOrCreatePlatform(String platformName) throws SQLException {
    Optional<GamePlatform> matching = allPlatforms.stream()
        .filter(platform -> platform.fullName.getValue().equals(platformName))
        .findFirst();
    if (matching.isPresent()) {
      return matching.get();
    } else {
      GamePlatform gamePlatform = new GamePlatform();
      gamePlatform.initializeForInsert();
      gamePlatform.fullName.changeValue(platformName);
      gamePlatform.commit(connection);
      allPlatforms.add(gamePlatform);
      return gamePlatform;
    }
  }

  private void handleGame(Integer igdb_id) throws SQLException {
    logger.info("Splitting platforms for IGDB_ID " + igdb_id);

    String sql = "SELECT * " +
        "FROM valid_game " +
        "WHERE igdb_id = ? " +
        "ORDER BY id ";
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, igdb_id);

    List<Game> matchingGames = new ArrayList<>();

    while (resultSet.next()) {
      Game game = new Game();
      game.initializeFromDBObject(resultSet);
      matchingGames.add(game);
    }

    if (shouldProcessGames(matchingGames)) {

      Game masterGame = chooseMainGame(matchingGames);

      List<AvailableGamePlatform> availableGamePlatforms = masterGame.getAvailableGamePlatforms(connection);

      for (Game matchingGame : matchingGames) {
        String platformName = matchingGame.platform.getValue();
        AvailableGamePlatform availablePlatform = maybeCreateAvailablePlatformFrom(masterGame, matchingGame, availableGamePlatforms);
        availableGamePlatforms.add(availablePlatform);

        moveForeignKeys(matchingGame, masterGame, availablePlatform, platformName);
        if (!matchingGame.id.getValue().equals(masterGame.id.getValue())) {
          removeForeignEntities(matchingGame);
        }
      }

      List<Game> dupes = new ArrayList<>(matchingGames);
      dupes.remove(masterGame);
      for (Game dupe : dupes) {
        if (!dupe.platform.getValue().equalsIgnoreCase(masterGame.platform.getValue())) {
          logger.info("Retiring platform " + dupe.platform.getValue());
          dupe.retire();
          dupe.commit(connection);
        }
      }

    }

  }

  private boolean shouldProcessGames(List<Game> matchingGames) {
    boolean hasOnlyOneGame = matchingGames.size() == 1;
    List<Game> steamGames = matchingGames.stream()
        .filter(game -> game.platform.getValue().equalsIgnoreCase("Steam"))
        .collect(Collectors.toList());
    Set<String> titles = matchingGames.stream()
        .map(game -> game.title.getValue())
        .collect(Collectors.toSet());
    return hasOnlyOneGame || (steamGames.size() == 1 && titles.size() == 1);
  }

  private Game chooseMainGame(List<Game> games) {
    Optional<Game> steamVersion = games.stream()
        .filter(game -> game.platform.getValue().equalsIgnoreCase("Steam"))
        .findFirst();
    return steamVersion.orElseGet(() -> games.get(0));
  }

  private AvailableGamePlatform maybeCreateAvailablePlatformFrom(Game masterGame, Game platformGame, List<AvailableGamePlatform> allAvailablePlatforms) throws SQLException {
    GamePlatform platform = getOrCreatePlatform(platformGame.platform.getValue());
    Integer gameID = masterGame.id.getValue();
    Integer platformID = platform.id.getValue();

    Optional<AvailableGamePlatform> existing = getAvailablePlatformFor(allAvailablePlatforms, gameID, platformID);

    if (existing.isPresent()) {
      return existing.get();
    } else {
      AvailableGamePlatform availableGamePlatform = new AvailableGamePlatform();
      availableGamePlatform.initializeForInsert();
      availableGamePlatform.gameID.changeValue(gameID);
      availableGamePlatform.gamePlatformID.changeValue(platformID);
      availableGamePlatform.platformName.changeValue(platform.fullName.getValue());
      availableGamePlatform.metacritic.changeValue(platformGame.metacritic.getValue());
      availableGamePlatform.metacritic_page.changeValue(platformGame.metacriticPage.getValue());
      availableGamePlatform.metacritic_matched.changeValue(platformGame.metacriticMatched.getValue());
      availableGamePlatform.commit(connection);

      return availableGamePlatform;
    }
  }

  @NotNull
  private Optional<AvailableGamePlatform> getAvailablePlatformFor(List<AvailableGamePlatform> allAvailablePlatforms, Integer gameID, Integer platformID) {
    return allAvailablePlatforms.stream()
        .filter(agp -> gameID.equals(agp.gameID.getValue()) && platformID.equals(agp.gamePlatformID.getValue()))
        .findFirst();
  }

  private void moveForeignKeys(Game oldGame, Game newGame, AvailableGamePlatform availableGamePlatform, String platformName) throws SQLException {
    Integer oldGameID = oldGame.id.getValue();
    Integer newGameID = newGame.id.getValue();
    Integer platformID = availableGamePlatform.id.getValue();
    moveGameLogs(oldGameID, newGameID, platformID, platformName);
    moveGameplaySessions(oldGameID, newGameID, platformID);
    moveSteamAttributes(oldGameID, newGameID);
  }

  private void removeForeignEntities(Game oldGame) throws SQLException {
    removeIGDBPosters(oldGame);
    removePossibleGameMatches(oldGame);
  }

  private void removeIGDBPosters(Game oldGame) throws SQLException {
    String sql = "DELETE FROM igdb_poster WHERE game_id = ? ";
    Integer rowsDeleted = connection.prepareAndExecuteStatementUpdate(sql, oldGame.id.getValue());
    logger.info(rowsDeleted + " rows deleted from IGDB_POSTER.");
  }

  private void removePossibleGameMatches(Game oldGame) throws SQLException {
    String sql = "DELETE FROM possible_game_match WHERE game_id = ? ";
    Integer rowsDeleted = connection.prepareAndExecuteStatementUpdate(sql, oldGame.id.getValue());
    logger.info(rowsDeleted + " rows deleted from POSSIBLE_GAME_MATCH.");
  }

  private void moveGameLogs(Integer oldGameID, Integer newGameID, Integer platformID, String platformName) throws SQLException {
    String sql = "UPDATE game_log " +
        "SET game_id = ?, available_game_platform_id = ?, platform = ? " +
        "WHERE game_id = ? ";
    Integer rowsUpdated = connection.prepareAndExecuteStatementUpdate(sql, newGameID, platformID, platformName, oldGameID);
    logger.info(rowsUpdated + " rows updated in GAME_LOG.");
  }

  private void moveGameplaySessions(Integer oldGameID, Integer newGameID, Integer platformID) throws SQLException {
    String sql = "UPDATE gameplay_session " +
        "SET game_id = ?, available_game_platform_id = ? " +
        "WHERE game_id = ? ";
    Integer rowsUpdated = connection.prepareAndExecuteStatementUpdate(sql, newGameID, platformID, oldGameID);
    logger.info(rowsUpdated + " rows updated in GAMEPLAY_SESSION.");
  }

  private void moveSteamAttributes(Integer oldGameID, Integer newGameID) throws SQLException {
    String sql = "UPDATE steam_attribute " +
        "SET game_id = ? " +
        "WHERE game_id = ? ";
    Integer rowsUpdated = connection.prepareAndExecuteStatementUpdate(sql, newGameID, oldGameID);
    logger.info(rowsUpdated + " rows updated in STEAM_ATTRIBUTE.");
  }


}
