package com.mayhew3.gamesutil.dataobject;

import com.sun.istack.internal.NotNull;
import org.apache.commons.lang3.ObjectUtils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

public abstract class FieldValue<T> {
  private String fieldName;
  private T originalValue;
  private T changedValue;
  private FieldConversion<T> converter;
  private Boolean explicitNull = false;

  Nullability nullability = Nullability.NULLABLE;

  private Boolean wasText = false;
  protected Boolean isText = false;

  public FieldValue(String fieldName, FieldConversion<T> converter, @NotNull Nullability nullability) {
    this.fieldName = fieldName;
    this.converter = converter;
    this.nullability = nullability;
  }

  public T getOriginalValue() {
    return originalValue;
  }

  public T getValue() {
    return changedValue;
  }

  protected void initializeValue(T value) {
    this.originalValue = value;
    this.changedValue = value;
  }

  protected void initializeValueFromString(String valueString) {
    T convertedValue = getConversion(valueString);
    initializeValue(convertedValue);

    this.wasText = true;
  }

  // todo: make abstract, and force all subtypes to implement.
  String getDDLType() {
    throw new UnsupportedOperationException("This method needs to be implemented on all subtypes that call it.");
  }

  abstract protected void initializeValue(ResultSet resultSet) throws SQLException;

  private T getConversion(String valueString) {
    try {
      return converter.parseFromString(valueString);
    } catch (NumberFormatException e) {
      throw new RuntimeException("Error converting " + fieldName + " field with value " + valueString + " to Number.");
    }
  }

  public void changeValueFromString(String valueString) {
    changeValue(getConversion(valueString));
  }

  public String getFieldName() {
    return fieldName;
  }


  public T getChangedValue() {
    return changedValue;
  }

  public void changeValue(T newValue) {
    if (newValue == null) {
      explicitNull = true;
    }
    changedValue = newValue;
  }

  public void changeValueUnlessToNull(T newValue) {
    if (newValue != null) {
      changeValue(newValue);
    }
  }

  public void nullValue() {
    changedValue = null;
  }

  public void discardChange() {
    changedValue = originalValue;
  }

  public Boolean isChanged() {
    return shouldUpgradeText() || valueHasChanged();
  }

  private boolean valueHasChanged() {
    return !Objects.equals(originalValue, changedValue);
  }

  protected boolean shouldUpgradeText() {
    return (originalValue != null && wasText && !isText);
  }

  public void updateInternal() {
    originalValue = changedValue;
  }

  @Override
  public String toString() {
    String displayString = "'" + fieldName + "': " + originalValue;
    if (isChanged()) {
      displayString += " -> " + changedValue;
    }
    return displayString;
  }

  public abstract void updatePreparedStatement(PreparedStatement preparedStatement, int currentIndex) throws SQLException;

  public Boolean getExplicitNull() {
    return explicitNull;
  }
}
