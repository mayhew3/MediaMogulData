package com.mayhew3.mediamogul.model.tv;

import com.mayhew3.postgresobject.dataobject.*;
import com.mayhew3.mediamogul.model.Person;

public class PersonSeries extends RetireableDataObject {

  public FieldValueForeignKey seriesId = registerForeignKey(new Series(), Nullability.NOT_NULL);
  public FieldValueForeignKey personId = registerForeignKey(new Person(), Nullability.NOT_NULL);

  FieldValueInteger rating = registerIntegerField("rating", Nullability.NULLABLE);
  public FieldValueInteger tier = registerIntegerField("tier", Nullability.NOT_NULL).defaultValue(1);

  FieldValueTimestamp ratingDate = registerTimestampField("rating_date", Nullability.NULLABLE);

  /* WATCHED DENORMS */
  public FieldValueInteger unwatchedEpisodes = registerIntegerField("unwatched_episodes", Nullability.NOT_NULL).defaultValue(0);
  public FieldValueInteger unwatchedStreaming = registerIntegerField("unwatched_streaming", Nullability.NOT_NULL).defaultValue(0);
  public FieldValueTimestamp firstUnwatched = registerTimestampField("first_unwatched", Nullability.NULLABLE);
  public FieldValueTimestamp lastUnwatched = registerTimestampField("last_unwatched", Nullability.NULLABLE);

  public FieldValueBoolean pinned = registerBooleanField("pinned", Nullability.NOT_NULL).defaultValue(false);

  public PersonSeries() {
    addUniqueConstraint(seriesId, personId);
  }

  @Override
  public String getTableName() {
    return "person_series";
  }

  @Override
  public String toString() {
    return "Person " + personId.getValue() + ", Series " + seriesId.getValue();
  }

}
