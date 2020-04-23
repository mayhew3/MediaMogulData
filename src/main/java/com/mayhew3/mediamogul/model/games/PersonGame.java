package com.mayhew3.mediamogul.model.games;

import com.mayhew3.mediamogul.model.Person;
import com.mayhew3.postgresobject.dataobject.*;
import com.mayhew3.postgresobject.db.SQLConnection;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class PersonGame extends RetireableDataObject {

  public FieldValueTimestamp finished_date = registerTimestampField("finished_date", Nullability.NULLABLE);
  public FieldValueTimestamp last_played = registerTimestampField("last_played", Nullability.NULLABLE);

  public FieldValueInteger minutes_played = registerIntegerField("minutes_played", Nullability.NOT_NULL);
  public FieldValueBigDecimal final_score = registerBigDecimalField("final_score", Nullability.NULLABLE);
  public FieldValueBigDecimal replay_score = registerBigDecimalField("replay_score", Nullability.NULLABLE);

  public FieldValueString replay_reason = registerStringField("replay_reason", Nullability.NULLABLE);

  public FieldValueForeignKey game_id = registerForeignKey(new Game(), Nullability.NOT_NULL);
  public FieldValueForeignKey person_id = registerForeignKey(new Person(), Nullability.NOT_NULL);

  public FieldValueBigDecimal rating = registerBigDecimalField("rating", Nullability.NULLABLE);

  public FieldValueInteger tier = registerIntegerField("tier", Nullability.NOT_NULL);

  public PersonGame() {
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
    String sql = "SELECT * FROM game WHERE id = ? ";
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, game_id.getValue());
    if (resultSet.next()) {
      Game game = new Game();
      game.initializeFromDBObject(resultSet);
      return game;
    }
    throw new IllegalStateException("PersonGame is attached to Game that doesn't exist!");
  }

  public List<GamePlatform> getMyPlatforms(SQLConnection connection) throws SQLException {
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
}
