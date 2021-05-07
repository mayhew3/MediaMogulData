package com.mayhew3.mediamogul.model.tv.group;

import com.mayhew3.postgresobject.dataobject.FieldValue;
import com.mayhew3.postgresobject.dataobject.FieldValueString;
import com.mayhew3.postgresobject.dataobject.Nullability;
import com.mayhew3.postgresobject.dataobject.RetireableDataObject;

import java.math.BigDecimal;

public class TVGroup extends RetireableDataObject {

  public FieldValueString name = registerStringField("name", Nullability.NULLABLE);
  public FieldValue<BigDecimal> minWeight = registerBigDecimalField("min_weight", Nullability.NOT_NULL).defaultValue(BigDecimal.valueOf(0.6));

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
