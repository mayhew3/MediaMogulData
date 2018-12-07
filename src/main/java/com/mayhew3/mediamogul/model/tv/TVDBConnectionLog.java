package com.mayhew3.mediamogul.model.tv;

import com.mayhew3.postgresobject.dataobject.*;

public class TVDBConnectionLog extends DataObject {

  public FieldValueTimestamp startTime = registerTimestampField("start_time", Nullability.NOT_NULL);
  public FieldValueTimestamp finishTime = registerTimestampField("finish_time", Nullability.NULLABLE);

  public FieldValueString updateType = registerStringField("update_type", Nullability.NOT_NULL);

  public FieldValueInteger updatedShows = registerIntegerField("updated_shows", Nullability.NOT_NULL).defaultValue(0);
  public FieldValueInteger failedShows = registerIntegerField("failed_shows", Nullability.NOT_NULL).defaultValue(0);

  @Override
  public String getTableName() {
    return "tvdb_connection_log";
  }
}
