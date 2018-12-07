package com.mayhew3.mediamogul.model.tv;

import com.mayhew3.postgresobject.dataobject.*;

public class Season extends RetireableDataObject {

  /* FK */
  public FieldValueForeignKey seriesId = registerForeignKey(new Series(), Nullability.NOT_NULL);

  /* Data */
  public FieldValueInteger seasonNumber = registerIntegerField("season_number", Nullability.NOT_NULL, IntegerSize.SMALLINT);
  public FieldValueInteger metacritic = registerIntegerField("metacritic", Nullability.NULLABLE, IntegerSize.SMALLINT);

  public FieldValueTimestamp dateModified = registerTimestampField("date_modified", Nullability.NULLABLE);

  public Season() {
    addUniqueConstraint(seasonNumber, seriesId);
  }

  @Override
  public String getTableName() {
    return "season";
  }

  @Override
  public String toString() {
    return seriesId.getValue() + ", Season " + seasonNumber.getValue();
  }

}
