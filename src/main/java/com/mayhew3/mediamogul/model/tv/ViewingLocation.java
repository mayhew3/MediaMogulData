package com.mayhew3.mediamogul.model.tv;

import com.mayhew3.postgresobject.dataobject.*;
import com.mayhew3.postgresobject.db.SQLConnection;
import org.jetbrains.annotations.NotNull;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ViewingLocation extends DataObject {

  /* Data */
  public FieldValueString viewingLocationName = registerStringField("name", Nullability.NOT_NULL);
  public FieldValueBoolean streaming = registerBooleanField("streaming", Nullability.NOT_NULL).defaultValue(true);

  @Override
  public String getTableName() {
    return "viewing_location";
  }

  @Override
  public String toString() {
    return viewingLocationName.getValue();
  }


  @NotNull
  static ViewingLocation findOrCreate(SQLConnection connection, String viewingLocationName) throws SQLException {
    ViewingLocation viewingLocation = new ViewingLocation();

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(
        "SELECT * FROM " + viewingLocation.getTableName() + " " +
            "WHERE " + viewingLocation.viewingLocationName.getFieldName() + " = ?",
        viewingLocationName);

    if (resultSet.next()) {
      viewingLocation.initializeFromDBObject(resultSet);
    } else {
      viewingLocation.initializeForInsert();
      viewingLocation.viewingLocationName.changeValue(viewingLocationName);
      viewingLocation.commit(connection);
    }

    return viewingLocation;
  }
}
