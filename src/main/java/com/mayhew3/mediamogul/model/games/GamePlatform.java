package com.mayhew3.mediamogul.model.games;

import com.mayhew3.postgresobject.dataobject.DataObject;
import com.mayhew3.postgresobject.dataobject.FieldValueInteger;
import com.mayhew3.postgresobject.dataobject.FieldValueString;
import com.mayhew3.postgresobject.dataobject.Nullability;

public class GamePlatform extends DataObject {

  public FieldValueString fullName = registerStringField("full_name", Nullability.NOT_NULL);
  public FieldValueString shortName = registerStringField("short_name", Nullability.NULLABLE);
  public FieldValueString igdbPlatformId = registerStringField("igdb_platform_id", Nullability.NULLABLE);
  public FieldValueString igdbName = registerStringField("igdb_name", Nullability.NULLABLE);
  public FieldValueInteger parentPlatformID = registerIntegerField("parent_id", Nullability.NULLABLE);

  @Override
  public String getTableName() {
    return "game_platform";
  }

  @Override
  public String toString() {
    return fullName.getValue();
  }

}
