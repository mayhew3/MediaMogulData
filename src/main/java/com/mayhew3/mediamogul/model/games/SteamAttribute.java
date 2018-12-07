package com.mayhew3.mediamogul.model.games;

import com.mayhew3.postgresobject.dataobject.*;

public class SteamAttribute extends DataObject {


  public FieldValueInteger steamID = registerIntegerField("steamid", Nullability.NOT_NULL);

  public FieldValueString attribute = registerStringField("attribute", Nullability.NOT_NULL);

  public FieldValueForeignKey gameID = registerForeignKey(new Game(), Nullability.NOT_NULL);

  @Override
  public String getTableName() {
    return "steam_attribute";
  }

  @Override
  public String toString() {
    return "Attribute '" + attribute.getValue() + "' for SteamID: " + steamID.getValue();
  }
}
