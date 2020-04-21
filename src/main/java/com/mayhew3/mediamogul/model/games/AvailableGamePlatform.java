package com.mayhew3.mediamogul.model.games;

import com.mayhew3.postgresobject.dataobject.DataObject;
import com.mayhew3.postgresobject.dataobject.FieldValueForeignKey;
import com.mayhew3.postgresobject.dataobject.Nullability;

public class AvailableGamePlatform extends DataObject {

  public FieldValueForeignKey gameID = registerForeignKey(new Game(), Nullability.NOT_NULL);
  public FieldValueForeignKey gamePlatformID = registerForeignKey(new GamePlatform(), Nullability.NOT_NULL);

  @Override
  public String getTableName() {
    return "available_game_platform";
  }

  @Override
  public String toString() {
    return "Platform ID " + gamePlatformID.getValue() + " for Game ID " + gameID.getValue();
  }

}
