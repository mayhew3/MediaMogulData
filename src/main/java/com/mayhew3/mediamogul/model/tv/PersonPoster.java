package com.mayhew3.mediamogul.model.tv;

import com.mayhew3.mediamogul.model.Person;
import com.mayhew3.postgresobject.dataobject.*;

public class PersonPoster extends RetireableDataObject {

  public FieldValueForeignKey seriesId = registerForeignKey(new Series(), Nullability.NOT_NULL);
  public FieldValueForeignKey personId = registerForeignKey(new Person(), Nullability.NOT_NULL);
  public FieldValueForeignKey tvdb_poster_id = registerForeignKey(new TVDBPoster(), Nullability.NOT_NULL);

  public PersonPoster() {
    addUniqueConstraint(seriesId, personId);
  }

  @Override
  public String getTableName() {
    return "person_poster";
  }

  @Override
  public String toString() {
    return "Custom Poster for Person " + personId.getValue() + ", Series " + seriesId.getValue();
  }

}
