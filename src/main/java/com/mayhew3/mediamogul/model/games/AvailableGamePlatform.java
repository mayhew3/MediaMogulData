package com.mayhew3.mediamogul.model.games;

import com.mayhew3.postgresobject.dataobject.*;
import com.mayhew3.postgresobject.db.SQLConnection;

import java.sql.ResultSet;
import java.sql.SQLException;

public class AvailableGamePlatform extends RetireableDataObject {

  public FieldValueForeignKey gameID = registerForeignKey(new Game(), Nullability.NOT_NULL);
  public FieldValueForeignKey gamePlatformID = registerForeignKey(new GamePlatform(), Nullability.NOT_NULL);
  public FieldValueString platformName = registerStringField("platform_name", Nullability.NOT_NULL);

  public FieldValueBigDecimal metacritic = registerBigDecimalField("metacritic", Nullability.NULLABLE);
  public FieldValueBoolean metacriticPage = registerBooleanField("metacritic_page", Nullability.NOT_NULL).defaultValue(false);
  public FieldValueTimestamp metacriticMatched = registerTimestampField("metacritic_matched", Nullability.NULLABLE);

  public AvailableGamePlatform() {
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
}
