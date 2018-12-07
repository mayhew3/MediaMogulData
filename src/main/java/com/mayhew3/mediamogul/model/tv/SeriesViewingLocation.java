package com.mayhew3.mediamogul.model.tv;

import com.mayhew3.postgresobject.dataobject.*;

public class SeriesViewingLocation extends RetireableDataObject {

  /* Data */
  public FieldValueForeignKey seriesId = registerForeignKey(new Series(), Nullability.NOT_NULL);
  public FieldValueForeignKey viewingLocationId = registerForeignKey(new ViewingLocation(), Nullability.NOT_NULL);

  @Override
  public String getTableName() {
    return "series_viewing_location";
  }

  @Override
  public String toString() {
    return seriesId.getValue() + ", " + viewingLocationId.getValue();
  }

}
