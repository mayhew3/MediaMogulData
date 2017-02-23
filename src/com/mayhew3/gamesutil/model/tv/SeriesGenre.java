package com.mayhew3.gamesutil.model.tv;

import com.mayhew3.gamesutil.dataobject.FieldValueForeignKey;
import com.mayhew3.gamesutil.dataobject.Nullability;

public class SeriesGenre extends RetireableDataObject {

  /* Data */
  public FieldValueForeignKey seriesId = registerForeignKey(new Series(), Nullability.NOT_NULL);
  public FieldValueForeignKey genreId = registerForeignKey(new Genre(), Nullability.NOT_NULL);

  public SeriesGenre() {
    addUniqueConstraint(seriesId, genreId);
  }

  @Override
  protected String getTableName() {
    return "series_genre";
  }

  @Override
  public String toString() {
    return seriesId.getValue() + ", " + genreId.getValue();
  }

}
