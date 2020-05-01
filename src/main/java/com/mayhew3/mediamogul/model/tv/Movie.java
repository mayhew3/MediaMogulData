package com.mayhew3.mediamogul.model.tv;

import com.mayhew3.postgresobject.dataobject.*;

public class Movie extends RetireableDataObject {

  public FieldValue<Boolean> onTiVo = registerBooleanField("on_tivo", Nullability.NOT_NULL).defaultValue(true);

  public FieldValueString title = registerStringField("title", Nullability.NOT_NULL);

  public FieldValueTimestamp watchedDate = registerTimestampField("watched_date", Nullability.NULLABLE);
  public FieldValue<Boolean> watched = registerBooleanField("watched", Nullability.NOT_NULL).defaultValue(false);

  public FieldValueInteger tier = registerIntegerField("tier", Nullability.NULLABLE);
  public FieldValueInteger metacritic = registerIntegerField("metacritic", Nullability.NULLABLE);
  public FieldValueInteger my_rating = registerIntegerField("my_rating", Nullability.NULLABLE);
  public FieldValueString metacriticHint = registerStringField("metacritic_hint", Nullability.NULLABLE);
  public FieldValueString tivoName = registerStringField("tivo_name", Nullability.NULLABLE);


  public FieldValueBoolean suggestion = registerBooleanField("suggestion", Nullability.NOT_NULL).defaultValue(false);

  public FieldValueTimestamp showingStartTime = registerTimestampField("showing_start_time", Nullability.NULLABLE);
  public FieldValueTimestamp deletedDate = registerTimestampField("deleted_date", Nullability.NULLABLE);
  public FieldValueTimestamp captureDate = registerTimestampField("capture_date", Nullability.NULLABLE);

  public FieldValue<Boolean> hd = registerBooleanField("hd", Nullability.NULLABLE);

  public FieldValue<Integer> duration = registerIntegerField("duration", Nullability.NULLABLE);
  public FieldValue<Integer> showingDuration = registerIntegerField("showing_duration", Nullability.NULLABLE);
  public FieldValue<Integer> channel = registerIntegerField("channel", Nullability.NULLABLE);
  public FieldValue<Integer> rating = registerIntegerField("rating", Nullability.NULLABLE);

  public FieldValue<String> tivoSeriesExtId = registerStringField("tivo_series_ext_id", Nullability.NULLABLE);
  public FieldValue<String> tivoSeriesV2ExtId = registerStringField("tivo_series_v2_ext_id", Nullability.NULLABLE);
  public FieldValue<String> programId = registerStringField("program_id", Nullability.NULLABLE);
  public FieldValue<String> programV2Id = registerStringField("program_v2_id", Nullability.NULLABLE);
  public FieldValue<String> seriesTitle = registerStringField("series_title", Nullability.NULLABLE);
  public FieldValue<String> description = registerStringField("description", Nullability.NULLABLE);
  public FieldValue<String> station = registerStringField("station", Nullability.NULLABLE);
  public FieldValue<String> url = registerStringField("url", Nullability.NULLABLE);

  public Movie() {
    super();
    addUniqueConstraint(title, captureDate, retired);
  }

  @Override
  public String getTableName() {
    return "movie";
  }

  @Override
  public String toString() {
    return title.getValue();
  }

}
