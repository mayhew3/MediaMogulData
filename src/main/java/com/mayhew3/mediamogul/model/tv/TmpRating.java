package com.mayhew3.mediamogul.model.tv;

import com.mayhew3.postgresobject.dataobject.*;

public class TmpRating extends DataObject {

  public FieldValueForeignKey seriesId = registerForeignKey(new Series(), Nullability.NULLABLE);
  public FieldValueForeignKey episodeId = registerForeignKey(new Episode(), Nullability.NULLABLE);

  public FieldValueString seriesTitle = registerStringField("series_title", Nullability.NOT_NULL);
  public FieldValueString episodeTitle = registerStringField("episode_title", Nullability.NULLABLE);
  public FieldValueInteger seasonNumber = registerIntegerField("season_number", Nullability.NULLABLE);
  public FieldValueInteger episodeNumber = registerIntegerField("episode_number", Nullability.NULLABLE);

  public FieldValueTimestamp watchedDate = registerTimestampField("watched_date", Nullability.NULLABLE);

  public FieldValueBigDecimal funny = registerBigDecimalField("funny", Nullability.NULLABLE);
  public FieldValueBigDecimal character = registerBigDecimalField("character", Nullability.NULLABLE);
  public FieldValueBigDecimal story = registerBigDecimalField("story", Nullability.NULLABLE);
  public FieldValueBigDecimal rating = registerBigDecimalField("rating", Nullability.NULLABLE);

  public FieldValueString note = registerStringField("note", Nullability.NULLABLE);


  @Override
  public String getTableName() {
    return "tmp_rating";
  }

  @Override
  public String toString() {
    return "Series: '" + seriesTitle.getValue() + "', " +
        "Episode " + seasonNumber.getValue() + "x" + episodeNumber.getValue() + ": " +
        "'" + episodeTitle.getValue() + "'";
  }
}
