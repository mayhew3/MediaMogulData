package com.mayhew3.gamesutil.dataobject;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

public class FieldValueString extends FieldValue<String> {
  public FieldValueString(String fieldName, FieldConversion<String> converter, Nullability nullability) {
    super(fieldName, converter, nullability);
  }

  @Override
  protected void initializeValue(String value) {
    super.initializeValue(value);
    this.isText = true;
  }

  @Override
  protected void initializeValueFromString(String valueString) {
    super.initializeValueFromString(valueString);
    this.isText = true;
  }

  @Override
  String getDDLType() {
    return "TEXT";
  }

  @Override
  String getInformationSchemaType() {
    return "text";
  }

  @Override
  protected void initializeValue(ResultSet resultSet) throws SQLException {
    initializeValue(resultSet.getString(getFieldName()));
  }

  @Override
  public void updatePreparedStatement(PreparedStatement preparedStatement, int currentIndex) throws SQLException {
    if (getChangedValue() == null) {
      preparedStatement.setNull(currentIndex, Types.VARCHAR);
    } else {
      preparedStatement.setString(currentIndex, getChangedValue());
    }
  }
}
