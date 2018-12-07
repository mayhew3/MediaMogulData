package com.mayhew3.mediamogul.model.tv;

import com.mayhew3.postgresobject.dataobject.Nullability;
import com.mayhew3.postgresobject.dataobject.RetireableDataObject;
import com.mayhew3.mediamogul.model.Person;

public class SeriesRequest extends RetireableDataObject {

  public SeriesRequest() {
    registerForeignKey(new Person(), Nullability.NOT_NULL);
    registerIntegerField("tvdb_series_ext_id", Nullability.NOT_NULL);
    registerStringField("title", Nullability.NOT_NULL);
    registerStringField("poster", Nullability.NULLABLE);
    registerTimestampField("approved", Nullability.NULLABLE);
    registerTimestampField("rejected", Nullability.NULLABLE);
    registerTimestampField("completed", Nullability.NULLABLE);
  }

  @Override
  public String getTableName() {
    return "series_request";
  }
}
