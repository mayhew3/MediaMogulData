package com.mayhew3.mediamogul.model.games;

import com.mayhew3.postgresobject.dataobject.*;
import com.mayhew3.mediamogul.model.Person;

public class GameplaySession extends RetireableDataObject {

  public FieldValueForeignKey gameID = registerForeignKey(new Game(), Nullability.NOT_NULL);

  public FieldValueTimestamp startTime = registerTimestampField("start_time", Nullability.NOT_NULL);

  public FieldValueInteger minutes = registerIntegerField("minutes", Nullability.NOT_NULL);
  public FieldValueInteger rating = registerIntegerField("rating", Nullability.NULLABLE);
  public FieldValueInteger manualAdjustment = registerIntegerField("manual_adjustment", Nullability.NOT_NULL).defaultValue(0);

  public FieldValueForeignKey person_id = registerForeignKey(new Person(), Nullability.NOT_NULL);
  public FieldValueForeignKey availableGamePlatform = registerForeignKey(new AvailableGamePlatform(), Nullability.NULLABLE);


  @Override
  public String getTableName() {
    return "gameplay_session";
  }
}
