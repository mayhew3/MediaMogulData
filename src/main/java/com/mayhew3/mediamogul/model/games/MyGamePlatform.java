package com.mayhew3.mediamogul.model.games;

import com.mayhew3.mediamogul.model.Person;
import com.mayhew3.postgresobject.dataobject.DataObject;
import com.mayhew3.postgresobject.dataobject.FieldValueForeignKey;
import com.mayhew3.postgresobject.dataobject.Nullability;

public class MyGamePlatform extends DataObject {

  public FieldValueForeignKey personID = registerForeignKey(new Person(), Nullability.NOT_NULL);
  public FieldValueForeignKey availableGamePlatformID = registerForeignKey(new AvailableGamePlatform(), Nullability.NOT_NULL);

  @Override
  public String getTableName() {
    return "my_game_platform";
  }

  @Override
  public String toString() {
    return "AvailableGamePlatform ID " + availableGamePlatformID.getValue() + " for Person ID " + personID.getValue();
  }

}
