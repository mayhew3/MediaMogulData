package com.mayhew3.mediamogul.model.tv;

import com.mayhew3.mediamogul.model.Person;
import com.mayhew3.postgresobject.dataobject.DataObject;
import com.mayhew3.postgresobject.dataobject.Nullability;

public class Friendship extends DataObject {

  public Friendship() {
    super();
    registerForeignKeyWithName(new Person(), Nullability.NOT_NULL, "hugging_person_id");
    registerForeignKeyWithName(new Person(), Nullability.NOT_NULL, "hugged_person_id");
    registerStringField("status", Nullability.NOT_NULL).defaultValue("pending");
  }

  @Override
  public String getTableName() {
    return "friendship";
  }
}
