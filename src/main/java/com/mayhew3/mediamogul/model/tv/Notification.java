package com.mayhew3.mediamogul.model.tv;

import com.mayhew3.mediamogul.model.Person;
import com.mayhew3.postgresobject.dataobject.DataObject;
import com.mayhew3.postgresobject.dataobject.Nullability;

public class Notification extends DataObject {

  public Notification() {
    super();
    registerForeignKey(new Person(), Nullability.NOT_NULL);
    registerStringField("message", Nullability.NOT_NULL);
    registerStringField("status", Nullability.NOT_NULL).defaultValue("pending");
  }

  @Override
  public String getTableName() {
    return "notification";
  }
}
