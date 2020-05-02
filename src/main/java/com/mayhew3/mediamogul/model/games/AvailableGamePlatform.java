package com.mayhew3.mediamogul.model.games;

import com.mayhew3.postgresobject.dataobject.*;
import com.mayhew3.postgresobject.db.SQLConnection;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class AvailableGamePlatform extends RetireableDataObject {

  public FieldValueForeignKey gameID = registerForeignKey(new Game(), Nullability.NOT_NULL);
  public FieldValueForeignKey gamePlatformID = registerForeignKey(new GamePlatform(), Nullability.NOT_NULL);
  public FieldValueString platformName = registerStringField("platform_name", Nullability.NOT_NULL);

  public FieldValueBigDecimal metacritic = registerBigDecimalField("metacritic", Nullability.NULLABLE);
  public FieldValueBoolean metacriticPage = registerBooleanField("metacritic_page", Nullability.NOT_NULL).defaultValue(false);
  public FieldValueTimestamp metacriticMatched = registerTimestampField("metacritic_matched", Nullability.NULLABLE);

  public AvailableGamePlatform() {
    super();
    addUniqueConstraint(gameID, gamePlatformID);
  }

  @Override
  public String getTableName() {
    return "available_game_platform";
  }

  @Override
  public String toString() {
    return "Platform ID " + gamePlatformID.getValue() + " for Game ID " + gameID.getValue();
  }


  public GamePlatform getGamePlatform(SQLConnection connection) throws SQLException {
    String sql = "SELECT * " +
        "FROM game_platform " +
        "WHERE id = ? ";
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, gamePlatformID.getValue());
    if (resultSet.next()) {
      GamePlatform gamePlatform = new GamePlatform();
      gamePlatform.initializeFromDBObject(resultSet);
      return gamePlatform;
    } else {
      throw new IllegalStateException("No gamePlatform found with id: " + gamePlatformID.getValue());
    }
  }

  @SuppressWarnings("UnusedReturnValue")
  public Optional<MyGamePlatform> getMyPlatform(SQLConnection connection, Integer person_id) throws SQLException {
    String sql = "SELECT mgp.* " +
        "FROM my_game_platform mgp " +
        "WHERE mgp.person_id = ? " +
        "AND mgp.available_game_platform_id = ? ";
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, person_id, id.getValue());
    if (resultSet.next()) {
      MyGamePlatform myPlatform = new MyGamePlatform();
      myPlatform.initializeFromDBObject(resultSet);
      return Optional.of(myPlatform);
    } else {
      return Optional.empty();
    }
  }

  public List<MyGamePlatform> getMyPlatforms(SQLConnection connection) throws SQLException {
    String sql = "SELECT mgp.* " +
        "FROM my_game_platform mgp " +
        "WHERE mgp.available_game_platform_id = ? ";
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, id.getValue());
    List<MyGamePlatform> myGamePlatforms = new ArrayList<>();
    while (resultSet.next()) {
      MyGamePlatform myPlatform = new MyGamePlatform();
      myPlatform.initializeFromDBObject(resultSet);
      myGamePlatforms.add(myPlatform);
    }
    return myGamePlatforms;
  }

  public Game getGame(SQLConnection connection) throws SQLException {
    String sql = "SELECT * FROM valid_game WHERE id = ? ";
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, gameID.getValue());
    if (resultSet.next()) {
      Game game = new Game();
      game.initializeFromDBObject(resultSet);
      return game;
    }
    throw new IllegalStateException("AvailableGamePlatform is attached to Game that doesn't exist!");
  }
}
