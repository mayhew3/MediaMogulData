package com.mayhew3.mediamogul.model.games;

import com.mayhew3.postgresobject.dataobject.*;

public class IGDBPoster extends DataObject {

  public FieldValueInteger igdb_game_id = registerIntegerField("igdb_game_id", Nullability.NULLABLE);
  public FieldValueString image_id = registerStringField("image_id", Nullability.NULLABLE);
  public FieldValueString url = registerStringField("url", Nullability.NULLABLE);
  public FieldValueInteger width = registerIntegerField("width", Nullability.NULLABLE);
  public FieldValueInteger height = registerIntegerField("height", Nullability.NULLABLE);
  public FieldValueBoolean default_for_game = registerBooleanField("default_for_game", Nullability.NOT_NULL).defaultValue(false);

  public FieldValueForeignKey game_id = registerForeignKey(new Game(), Nullability.NOT_NULL);

  @Override
  public String getTableName() {
    return "igdb_poster";
  }

}
