package com.mayhew3.gamesutil.dataobject;

public class PossibleSeriesMatch extends DataObject {

  /* FK */
  public FieldValueInteger seriesId = registerIntegerField("series_id", Nullability.NOT_NULL);

  /* Data */
  public FieldValueInteger tvdbSeriesId = registerIntegerField("tvdb_series_id", Nullability.NOT_NULL);
  public FieldValueString tvdbSeriesTitle = registerStringField("tvdb_series_title", Nullability.NOT_NULL);

  @Override
  protected String getTableName() {
    return "possible_series_match";
  }

  @Override
  public String toString() {
    return seriesId.getValue() + ", Title " + tvdbSeriesTitle.getValue();
  }

}
