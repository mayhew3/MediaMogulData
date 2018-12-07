package com.mayhew3.mediamogul.model.tv;

import com.mayhew3.postgresobject.dataobject.*;

public class EpisodeGroupRating extends RetireableDataObject {

  public FieldValueForeignKey seriesId = registerForeignKey(new Series(), Nullability.NOT_NULL);

  public FieldValueInteger year = registerIntegerField("year", Nullability.NOT_NULL);

  public FieldValueTimestamp startDate = registerTimestampField("start_date", Nullability.NOT_NULL);
  public FieldValueTimestamp endDate = registerTimestampField("end_date", Nullability.NOT_NULL);
  public FieldValueTimestamp lastAired = registerTimestampField("last_aired", Nullability.NULLABLE);
  public FieldValueTimestamp nextAirDate = registerTimestampField("next_air_date", Nullability.NULLABLE);
  public FieldValueTimestamp reviewUpdateDate = registerTimestampField("review_update_date", Nullability.NULLABLE);

  public FieldValueBigDecimal avgRating = registerBigDecimalField("avg_rating", Nullability.NULLABLE);
  public FieldValueBigDecimal maxRating = registerBigDecimalField("max_rating", Nullability.NULLABLE);
  public FieldValueBigDecimal lastRating = registerBigDecimalField("last_rating", Nullability.NULLABLE);
  public FieldValueBigDecimal suggestedRating = registerBigDecimalField("suggested_rating", Nullability.NULLABLE);

  public FieldValueBigDecimal avgFunny = registerBigDecimalField("avg_funny", Nullability.NULLABLE);
  public FieldValueBigDecimal avgCharacter = registerBigDecimalField("avg_character", Nullability.NULLABLE);
  public FieldValueBigDecimal avgStory = registerBigDecimalField("avg_story", Nullability.NULLABLE);

  public FieldValueInteger numEpisodes = registerIntegerField("num_episodes", Nullability.NOT_NULL);
  public FieldValueInteger watched = registerIntegerField("watched", Nullability.NOT_NULL);
  public FieldValueInteger rated = registerIntegerField("rated", Nullability.NOT_NULL);
  public FieldValueInteger aired = registerIntegerField("aired", Nullability.NOT_NULL);
  public FieldValueInteger postUpdateEpisodes = registerIntegerField("post_update_episodes", Nullability.NOT_NULL);

  public FieldValueBigDecimal rating = registerBigDecimalField("rating", Nullability.NULLABLE);

  public FieldValueString review = registerStringField("review", Nullability.NULLABLE);

  @Override
  public String getTableName() {
    return "episode_group_rating";
  }
}
