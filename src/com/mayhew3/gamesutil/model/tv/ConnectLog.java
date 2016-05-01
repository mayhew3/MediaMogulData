package com.mayhew3.gamesutil.model.tv;

import com.mayhew3.gamesutil.dataobject.*;

public class ConnectLog extends DataObject {

  public FieldValueTimestamp startTime = registerTimestampField("start_time", Nullability.NOT_NULL);
  public FieldValueTimestamp endTime = registerTimestampField("end_time", Nullability.NULLABLE);

  public FieldValueInteger addedShows = registerIntegerField("added_shows", Nullability.NULLABLE);
  public FieldValueInteger connectionID = registerIntegerField("connection_id", Nullability.NULLABLE);
  public FieldValueInteger deletedShows = registerIntegerField("deleted_shows", Nullability.NULLABLE);
  public FieldValueInteger tvdbEpisodesAdded = registerIntegerField("tvdb_episodes_added", Nullability.NULLABLE);
  public FieldValueInteger tvdbEpisodesUpdated = registerIntegerField("tvdb_episodes_updated", Nullability.NULLABLE);
  public FieldValueInteger tvdbSeriesUpdated = registerIntegerField("tvdb_series_updated", Nullability.NULLABLE);
  public FieldValueInteger timeConnected = registerIntegerField("time_connected", Nullability.NULLABLE);
  public FieldValueInteger updatedShows = registerIntegerField("updated_shows", Nullability.NULLABLE);

  public FieldValueBoolean fastUpdate = registerBooleanField("fast_update", Nullability.NOT_NULL).defaultValue(true);

  @Override
  protected String getTableName() {
    return "connect_log";
  }

  @Override
  public String toString() {
   return "Connectlog: " + startTime.getValue() + ", fast: " + fastUpdate.getValue();
  }
}
