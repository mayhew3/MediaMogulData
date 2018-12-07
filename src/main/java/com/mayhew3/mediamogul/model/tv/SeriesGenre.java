package com.mayhew3.mediamogul.model.tv;

import com.mayhew3.postgresobject.dataobject.FieldValueForeignKey;
import com.mayhew3.postgresobject.dataobject.Nullability;
import com.mayhew3.postgresobject.dataobject.RetireableDataObject;

public class SeriesGenre extends RetireableDataObject {

  /* Data */
  public FieldValueForeignKey seriesId = registerForeignKey(new Series(), Nullability.NOT_NULL);
  public FieldValueForeignKey genreId = registerForeignKey(new Genre(), Nullability.NOT_NULL);

  public SeriesGenre() {
    addUniqueConstraint(seriesId, genreId);
  }

  @Override
  public String getTableName() {
    return "series_genre";
  }

  @Override
  public String toString() {
    return seriesId.getValue() + ", " + genreId.getValue();
  }

}
