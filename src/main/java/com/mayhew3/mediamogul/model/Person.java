package com.mayhew3.mediamogul.model;

import com.mayhew3.postgresobject.dataobject.FieldValueBoolean;
import com.mayhew3.postgresobject.dataobject.FieldValueString;
import com.mayhew3.postgresobject.dataobject.Nullability;
import com.mayhew3.postgresobject.dataobject.RetireableDataObject;

public class Person extends RetireableDataObject {

  public FieldValueString userRole = registerStringField("user_role", Nullability.NOT_NULL).defaultValue("user");
  public FieldValueBoolean rating_notifications = registerBooleanField("rating_notifications", Nullability.NOT_NULL).defaultValue(false);
  public FieldValueString email = registerStringField("email", Nullability.NOT_NULL);
  public FieldValueString firstName = registerStringField("first_name", Nullability.NOT_NULL);
  public FieldValueString lastName = registerStringField("last_name", Nullability.NOT_NULL);

  public Person() {
    super();
    addUniqueConstraint(email);
  }

  @Override
  public String getTableName() {
    return "person";
  }

  @Override
  public String toString() {
    return email.getValue();
  }

}
