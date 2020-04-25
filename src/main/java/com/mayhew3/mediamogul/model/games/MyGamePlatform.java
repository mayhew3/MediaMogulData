package com.mayhew3.mediamogul.model.games;

import com.mayhew3.mediamogul.model.Person;
import com.mayhew3.postgresobject.dataobject.*;

public class MyGamePlatform extends DataObject {

  public FieldValueForeignKey personID = registerForeignKey(new Person(), Nullability.NOT_NULL);
  public FieldValueForeignKey availableGamePlatformID = registerForeignKey(new AvailableGamePlatform(), Nullability.NOT_NULL);

  public FieldValueString platformName = registerStringField("platform_name", Nullability.NOT_NULL);

  // Ratings
  public FieldValueBigDecimal rating = registerBigDecimalField("rating", Nullability.NULLABLE);
  public FieldValueInteger tier = registerIntegerField("tier", Nullability.NULLABLE);

  // Playtime
  public FieldValueTimestamp last_played = registerTimestampField("last_played", Nullability.NULLABLE);
  public FieldValueInteger minutes_played = registerIntegerField("minutes_played", Nullability.NOT_NULL);

  // Finished
  public FieldValueTimestamp finished_date = registerTimestampField("finished_date", Nullability.NULLABLE);
  public FieldValueBigDecimal final_score = registerBigDecimalField("final_score", Nullability.NULLABLE);
  public FieldValueBigDecimal replay_score = registerBigDecimalField("replay_score", Nullability.NULLABLE);
  public FieldValueString replay_reason = registerStringField("replay_reason", Nullability.NULLABLE);

  @Override
  public String getTableName() {
    return "my_game_platform";
  }

  @Override
  public String toString() {
    return "AvailableGamePlatform ID " + availableGamePlatformID.getValue() + " for Person ID " + personID.getValue();
  }

}
