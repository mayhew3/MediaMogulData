package com.mayhew3.mediamogul.games.utility;

import com.mayhew3.mediamogul.model.games.*;
import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class GamePlatformUpdater {
  private final SQLConnection connection;

  private static final Logger logger = LogManager.getLogger(GamePlatformUpdater.class);

  private final List<GamePlatform> allPlatforms = new ArrayList<>();

  private GamePlatformUpdater(SQLConnection connection) {
    this.connection = connection;
  }

  public static void main(String[] args) throws URISyntaxException, SQLException {
    ArgumentChecker argumentChecker = new ArgumentChecker(args);

    SQLConnection connection = PostgresConnectionFactory.createConnection(argumentChecker);
    GamePlatformUpdater updater = new GamePlatformUpdater(connection);
    updater.runUpdate();
  }

  private void runUpdate() throws SQLException {
    populateAllPlatforms();

    String sql = "SELECT DISTINCT igdb_id " +
        "FROM game " +
        "WHERE igdb_success IS NOT NULL " +
        "AND igdb_ignored IS NULL ";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql);
    while (resultSet.next()) {
      Integer igdb_id = resultSet.getInt("igdb_id");
      handleIGDBID(igdb_id);
    }
  }

  private void populateAllPlatforms() throws SQLException {
    String sql = "SELECT * " +
        "FROM game_platform ";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql);

    while (resultSet.next()) {
      GamePlatform gamePlatform = new GamePlatform();
      gamePlatform.initializeFromDBObject(resultSet);
      allPlatforms.add(gamePlatform);
    }
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

  private void handleIGDBID(Integer igdb_id) throws SQLException {
    logger.info("Splitting platforms for IGDB_ID " + igdb_id);

    String sql = "SELECT * " +
        "FROM game " +
        "WHERE igdb_id = ? " +
        "ORDER BY id ";
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, igdb_id);

    List<Game> matchingGames = new ArrayList<>();

    while (resultSet.next()) {
      Game game = new Game();
      game.initializeFromDBObject(resultSet);
      matchingGames.add(game);
    }

    Game masterGame = chooseMainGame(matchingGames);

    List<AvailableGamePlatform> availableGamePlatforms = masterGame.getAvailableGamePlatforms(connection);
    List<MyGamePlatform> allPersonGamePlatforms = masterGame.getAllPersonGamePlatforms(connection);

    for (Game matchingGame : matchingGames) {
      AvailableGamePlatform availablePlatform = createAvailablePlatformFrom(masterGame, matchingGame, availableGamePlatforms);
      List<PersonGame> personGames = matchingGame.getPersonGames(connection);
      for (PersonGame personGame : personGames) {
        addToMyPlatforms(availablePlatform, personGame, allPersonGamePlatforms);
      }
    }

  }

  private Game chooseMainGame(List<Game> games) {
    return games.get(0);
  }

  private AvailableGamePlatform createAvailablePlatformFrom(Game masterGame, Game platformGame, List<AvailableGamePlatform> allAvailablePlatforms) throws SQLException {
    GamePlatform platform = getOrCreatePlatform(platformGame.platform.getValue());
    Integer gameID = masterGame.id.getValue();
    Integer platformID = platform.id.getValue();

    Optional<AvailableGamePlatform> existing = allAvailablePlatforms.stream()
        .filter(agp -> gameID.equals(agp.gameID.getValue()) && platformID.equals(agp.gamePlatformID.getValue()))
        .findFirst();

    if (existing.isPresent()) {
      return existing.get();
    } else {
      AvailableGamePlatform availableGamePlatform = new AvailableGamePlatform();
      availableGamePlatform.initializeForInsert();
      availableGamePlatform.gameID.changeValue(gameID);
      availableGamePlatform.gamePlatformID.changeValue(platformID);
      availableGamePlatform.commit(connection);

      return availableGamePlatform;
    }
  }

  private MyGamePlatform addToMyPlatforms(AvailableGamePlatform availableGamePlatform, PersonGame personGame, List<MyGamePlatform> allPersonPlatforms) throws SQLException {
    Integer agpID = availableGamePlatform.id.getValue();
    Integer personID = personGame.person_id.getValue();

    Optional<MyGamePlatform> existing = allPersonPlatforms.stream()
        .filter(mgp -> agpID.equals(mgp.availableGamePlatformID.getValue()) && personID.equals(mgp.personID.getValue()))
        .findFirst();

    if (existing.isPresent()) {
      return existing.get();
    } else {
      MyGamePlatform myGamePlatform = new MyGamePlatform();
      myGamePlatform.initializeForInsert();

      myGamePlatform.availableGamePlatformID.changeValue(agpID);
      myGamePlatform.personID.changeValue(personID);
      myGamePlatform.commit(connection);

      return myGamePlatform;
    }
  }

  private void moveForeignKeys(Game oldGame, Game newGame) {
    // game logs
    // gameplay sessions
    // igdb posters?
    // steam attribute
  }
}
