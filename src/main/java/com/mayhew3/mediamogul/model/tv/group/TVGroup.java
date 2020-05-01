package com.mayhew3.mediamogul.model.tv.group;

import com.mayhew3.postgresobject.dataobject.FieldValueString;
import com.mayhew3.postgresobject.dataobject.Nullability;
import com.mayhew3.postgresobject.dataobject.RetireableDataObject;

public class TVGroup extends RetireableDataObject {

  private FieldValueString name = registerStringField("name", Nullability.NULLABLE);

  public TVGroup() {
    super();
    addUniqueConstraint(name);
  }

  @Override
  public String getTableName() {
    return "tv_group";
  }

  @Override
  public String toString() {
    return name.getValue() + ": ID " + id.getValue();
  }

}
