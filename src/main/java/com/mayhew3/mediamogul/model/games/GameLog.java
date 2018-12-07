package com.mayhew3.mediamogul.model.games;

import com.mayhew3.postgresobject.dataobject.*;
import com.mayhew3.postgresobject.db.SQLConnection;
import com.mayhew3.mediamogul.games.GameFailedException;
import com.mayhew3.mediamogul.model.Person;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Optional;

public class GameLog extends DataObject {


  public FieldValueTimestamp eventdate = registerTimestampField("eventdate", Nullability.NOT_NULL);

  public FieldValueBigDecimal previousPlaytime = registerBigDecimalField("previousplaytime", Nullability.NULLABLE);
  public FieldValueBigDecimal updatedplaytime = registerBigDecimalField("updatedplaytime", Nullability.NULLABLE);
  public FieldValueBigDecimal diff = registerBigDecimalField("diff", Nullability.NULLABLE);

  public FieldValueInteger steamID = registerIntegerField("steamid", Nullability.NULLABLE);

  public FieldValueString game = registerStringField("game", Nullability.NOT_NULL);
  public FieldValueString platform = registerStringField("platform", Nullability.NOT_NULL);
  public FieldValueString eventtype = registerStringField("eventtype", Nullability.NULLABLE);

  public FieldValueForeignKey gameID = registerForeignKey(new Game(), Nullability.NOT_NULL);

  public FieldValueForeignKey gameplaySessionID = registerForeignKey(new GameplaySession(), Nullability.NULLABLE);

  public FieldValueForeignKey person_id = registerForeignKey(new Person(), Nullability.NOT_NULL);

  @Override
  public String getTableName() {
    return "game_log";
  }

  @Override
  public String toString() {
    return game.getValue() + ": " + eventdate.getValue() + " (" + diff.getValue() + " minutes)";
  }

  public Optional<GameplaySession> getGameplaySession(SQLConnection connection) throws SQLException {
    if (gameplaySessionID.getValue() == null) {
      return Optional.empty();
    }

    String sql =
        "SELECT * FROM gameplay_session " +
        "WHERE id = ? ";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, gameplaySessionID.getValue());

    GameplaySession gameplaySession = new GameplaySession();

    if (resultSet.next()) {
      gameplaySession.initializeFromDBObject(resultSet);
      return Optional.of(gameplaySession);
    } else {
      throw new RuntimeException("GameLog with reference to invalid gameplay_session: ID " + gameplaySessionID.getValue());
    }
  }
}
