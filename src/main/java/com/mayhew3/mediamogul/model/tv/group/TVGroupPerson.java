package com.mayhew3.mediamogul.model.tv.group;

import com.mayhew3.postgresobject.dataobject.FieldValueForeignKey;
import com.mayhew3.postgresobject.dataobject.Nullability;
import com.mayhew3.postgresobject.dataobject.RetireableDataObject;
import com.mayhew3.mediamogul.model.Person;

public class TVGroupPerson extends RetireableDataObject {

  /* Data */
  private FieldValueForeignKey tv_group_id = registerForeignKey(new TVGroup(), Nullability.NOT_NULL);
  private FieldValueForeignKey person_id = registerForeignKey(new Person(), Nullability.NOT_NULL);

  public TVGroupPerson() {
    addUniqueConstraint(tv_group_id, person_id);
  }

  @Override
  public String getTableName() {
    return "tv_group_person";
  }

  @Override
  public String toString() {
    return "tv_group_person " + tv_group_id.getValue() + ", " + person_id.getValue();
  }

}
