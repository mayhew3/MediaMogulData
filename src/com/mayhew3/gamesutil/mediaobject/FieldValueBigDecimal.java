package com.mayhew3.gamesutil.mediaobject;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

public class FieldValueBigDecimal extends FieldValue<BigDecimal> {
  public FieldValueBigDecimal(String fieldName, FieldConversion<BigDecimal> converter) {
    super(fieldName, converter);
  }

  @Override
  public void updatePreparedStatement(PreparedStatement preparedStatement, int currentIndex) throws SQLException {
    if (getChangedValue() == null) {
      preparedStatement.setNull(currentIndex, Types.NUMERIC);
    } else {
      preparedStatement.setBigDecimal(currentIndex, getChangedValue());
    }
  }

  public void changeValue(Double newValue) {
    if (newValue == null) {
      changeValue((BigDecimal) null);
    } else {
      changeValue(BigDecimal.valueOf(newValue));
    }
  }
}
