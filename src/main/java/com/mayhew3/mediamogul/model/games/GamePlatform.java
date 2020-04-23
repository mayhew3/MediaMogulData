package com.mayhew3.mediamogul.model.games;

import com.mayhew3.postgresobject.dataobject.DataObject;
import com.mayhew3.postgresobject.dataobject.FieldValueInteger;
import com.mayhew3.postgresobject.dataobject.FieldValueString;
import com.mayhew3.postgresobject.dataobject.Nullability;
import com.mayhew3.postgresobject.db.SQLConnection;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class GamePlatform extends DataObject {

  public FieldValueString fullName = registerStringField("full_name", Nullability.NOT_NULL);
  public FieldValueString shortName = registerStringField("short_name", Nullability.NULLABLE);
  public FieldValueInteger igdbPlatformId = registerIntegerField("igdb_platform_id", Nullability.NULLABLE);
  public FieldValueString igdbName = registerStringField("igdb_name", Nullability.NULLABLE);
  public FieldValueInteger parentPlatformID = registerIntegerField("parent_id", Nullability.NULLABLE);

  public GamePlatform() {
    super();
    addUniqueConstraint(fullName);
  }

  @Override
  public String getTableName() {
    return "game_platform";
  }

  @Override
  public String toString() {
    return fullName.getValue();
  }

  public static List<GamePlatform> getAllPlatforms(SQLConnection connection) throws SQLException {

    String sql = "SELECT * " +
        "FROM game_platform ";

    List<GamePlatform> platforms = new ArrayList<>();

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql);

    while (resultSet.next()) {
      GamePlatform gamePlatform = new GamePlatform();
      gamePlatform.initializeFromDBObject(resultSet);
      platforms.add(gamePlatform);
    }

    return platforms;
  }
}
