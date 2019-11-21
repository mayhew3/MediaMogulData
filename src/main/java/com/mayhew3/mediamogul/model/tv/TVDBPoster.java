package com.mayhew3.mediamogul.model.tv;

import com.mayhew3.postgresobject.dataobject.FieldValueInteger;
import com.mayhew3.postgresobject.dataobject.FieldValueString;
import com.mayhew3.postgresobject.dataobject.Nullability;
import com.mayhew3.postgresobject.dataobject.RetireableDataObject;

public class TVDBPoster extends RetireableDataObject {

  public FieldValueString posterPath = registerStringField("poster_path", Nullability.NOT_NULL);
  public FieldValueString cloud_poster = registerStringField("cloud_poster", Nullability.NULLABLE);
  FieldValueInteger tvdb_series_id = registerForeignKey(new TVDBSeries(), Nullability.NOT_NULL);
  public FieldValueInteger season = registerIntegerField("season", Nullability.NULLABLE);

  public TVDBPoster() {
    super();
    registerTimestampField("hidden", Nullability.NULLABLE);
  }

  @Override
  public String getTableName() {
    return "tvdb_poster";
  }
}
