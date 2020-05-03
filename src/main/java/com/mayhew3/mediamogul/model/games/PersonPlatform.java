package com.mayhew3.mediamogul.model.games;

import com.mayhew3.mediamogul.model.Person;
import com.mayhew3.postgresobject.dataobject.*;

public class PersonPlatform extends DataObject {

  public FieldValueForeignKey personID = registerForeignKey(new Person(), Nullability.NOT_NULL);
  public FieldValueForeignKey gamePlatformID = registerForeignKey(new GamePlatform(), Nullability.NOT_NULL);

  public FieldValueString platformName = registerStringField("platform_name", Nullability.NOT_NULL);
  public FieldValueInteger rank = registerIntegerField("rank", Nullability.NOT_NULL);

  public PersonPlatform() {
    super();
    addUniqueConstraint(personID, gamePlatformID, rank);
  }

  @Override
  public String getTableName() {
    return "person_platform";
  }

  @Override
  public String toString() {
    return "GamePlatform ID " + gamePlatformID.getValue() + " for Person ID " + personID.getValue();
  }

}
