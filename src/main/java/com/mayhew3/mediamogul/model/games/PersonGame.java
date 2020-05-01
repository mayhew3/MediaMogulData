package com.mayhew3.mediamogul.model.games;

import com.mayhew3.mediamogul.model.Person;
import com.mayhew3.postgresobject.dataobject.*;
import com.mayhew3.postgresobject.db.SQLConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class PersonGame extends RetireableDataObject {

  // FKs
  public FieldValueForeignKey game_id = registerForeignKey(new Game(), Nullability.NOT_NULL);
  public FieldValueForeignKey person_id = registerForeignKey(new Person(), Nullability.NOT_NULL);

  // Ratings
  public FieldValueBigDecimal rating = registerBigDecimalField("rating", Nullability.NULLABLE);
  public FieldValueInteger tier = registerIntegerField("tier", Nullability.NOT_NULL);

  // Playtime
  public FieldValueTimestamp last_played = registerTimestampField("last_played", Nullability.NULLABLE);
  public FieldValueInteger minutes_played = registerIntegerField("minutes_played", Nullability.NOT_NULL);

  // Finished
  public FieldValueTimestamp finished_date = registerTimestampField("finished_date", Nullability.NULLABLE);
  public FieldValueBigDecimal final_score = registerBigDecimalField("final_score", Nullability.NULLABLE);
  public FieldValueBigDecimal replay_score = registerBigDecimalField("replay_score", Nullability.NULLABLE);
  public FieldValueString replay_reason = registerStringField("replay_reason", Nullability.NULLABLE);

  private static final Logger logger = LogManager.getLogger(PersonGame.class);


  public PersonGame() {
    super();
    addUniqueConstraint(game_id, person_id, retired);
  }

  @Override
  public String getTableName() {
    return "person_game";
  }

  @Override
  public String toString() {
    return "Person ID: " + person_id.getValue() + ", Game ID: " + game_id.getValue();
  }

  public Game getGame(SQLConnection connection) throws SQLException {
    String sql = "SELECT * FROM valid_game WHERE id = ? ";
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, game_id.getValue());
    if (resultSet.next()) {
      Game game = new Game();
      game.initializeFromDBObject(resultSet);
      return game;
    }
    throw new IllegalStateException("PersonGame is attached to Game that doesn't exist!");
  }

  public List<GamePlatform> getPlatforms(SQLConnection connection) throws SQLException {
    String sql = "SELECT p.* " +
        "FROM game_platform p " +
        "INNER JOIN available_game_platform agp " +
        " ON agp.game_platform_id = p.id " +
        "INNER JOIN my_game_platform mgp " +
        " ON mgp.available_game_platform_id = agp.id " +
        "WHERE mgp.person_id = ? " +
        "AND agp.game_id = ? ";
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, person_id.getValue(), game_id.getValue());
    List<GamePlatform> gamePlatforms = new ArrayList<>();
    while (resultSet.next()) {
      GamePlatform gamePlatform = new GamePlatform();
      gamePlatform.initializeFromDBObject(resultSet);
      gamePlatforms.add(gamePlatform);
    }
    return gamePlatforms;
  }

  @SuppressWarnings("UnusedReturnValue")
  public MyGamePlatform getOrCreatePlatform(SQLConnection connection, AvailableGamePlatform availableGamePlatform) throws SQLException {
    List<MyGamePlatform> myPlatforms = getMyPlatforms(connection);
    Optional<MyGamePlatform> existing = myPlatforms.stream()
        .filter(myPlatform -> myPlatform.availableGamePlatformID.getValue().equals(availableGamePlatform.id.getValue()))
        .findFirst();

    if (existing.isPresent()) {
      return existing.get();
    } else {
      MyGamePlatform myPlatform = new MyGamePlatform();
      myPlatform.initializeForInsert();
      myPlatform.availableGamePlatformID.changeValue(availableGamePlatform.id.getValue());
      myPlatform.personID.changeValue(person_id.getValue());
      myPlatform.platformName.changeValue(availableGamePlatform.platformName.getValue());
      myPlatform.commit(connection);
      return myPlatform;
    }
  }

  public void deleteMyPlatform(SQLConnection connection, AvailableGamePlatform availableGamePlatform) throws SQLException {
    String sql = "DELETE " +
        "FROM my_game_platform " +
        "WHERE available_game_platform_id = ? " +
        "AND person_id = ? ";
    Integer rowsDeleted = connection.prepareAndExecuteStatementUpdate(sql, availableGamePlatform.id.getValue(), person_id.getValue());
    logger.info(rowsDeleted + " rows deleted from my_game_platform for Game " + availableGamePlatform.gameID.getValue() + " on platform " + availableGamePlatform.platformName.getValue());
  }

  public List<MyGamePlatform> getMyPlatforms(SQLConnection connection) throws SQLException {
    String sql = "SELECT mgp.* " +
        "FROM game_platform p " +
        "INNER JOIN available_game_platform agp " +
        " ON agp.game_platform_id = p.id " +
        "INNER JOIN my_game_platform mgp " +
        " ON mgp.available_game_platform_id = agp.id " +
        "WHERE mgp.person_id = ? " +
        "AND agp.game_id = ? ";
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, person_id.getValue(), game_id.getValue());
    List<MyGamePlatform> myPlatforms = new ArrayList<>();
    while (resultSet.next()) {
      MyGamePlatform myPlatform = new MyGamePlatform();
      myPlatform.initializeFromDBObject(resultSet);
      myPlatforms.add(myPlatform);
    }
    return myPlatforms;
  }
}
