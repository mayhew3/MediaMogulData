package com.mayhew3.mediamogul.model.tv;

import com.mayhew3.postgresobject.dataobject.*;

public class ErrorLog extends DataObject {

  /* Data */
  public FieldValueString chosenName = registerStringField("chosen_name", Nullability.NULLABLE);
  public FieldValueString errorMessage = registerStringField("error_message", Nullability.NULLABLE);
  public FieldValueString errorType = registerStringField("error_type", Nullability.NULLABLE);
  public FieldValueTimestamp eventDate = registerTimestampField("event_date", Nullability.NULLABLE);
  public FieldValueString formattedName = registerStringField("formatted_name", Nullability.NULLABLE);
  public FieldValueBoolean resolved = registerBooleanField("resolved", Nullability.NULLABLE);
  public FieldValueTimestamp resolvedDate = registerTimestampField("resolved_date", Nullability.NULLABLE);
  public FieldValueString tvdbName = registerStringField("tvdb_name", Nullability.NULLABLE);
  public FieldValueString tivoId = registerStringField("tivo_id", Nullability.NULLABLE);
  public FieldValueString tivoName = registerStringField("tivo_name", Nullability.NULLABLE);
  public FieldValueString context = registerStringField("context", Nullability.NULLABLE);
  public FieldValueBoolean ignoreError = registerBooleanField("ignore_error", Nullability.NULLABLE);

  @Override
  public String getTableName() {
    return "error_log";
  }

  @Override
  public String toString() {
    return "Error type '" + errorType.getValue() + "', Message: " + errorMessage.getValue();
  }

}
