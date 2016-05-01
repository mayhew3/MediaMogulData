package com.mayhew3.gamesutil.dataobject;

import com.mayhew3.gamesutil.db.SQLConnection;
import com.sun.istack.internal.NotNull;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ViewingLocation extends DataObject {

  /* Data */
  public FieldValueString viewingLocationName = registerStringField("name", Nullability.NOT_NULL);
  public FieldValueBoolean streaming = registerBooleanField("streaming", Nullability.NOT_NULL).defaultValue(true);

  public ViewingLocation() {
    id.setSize(IntegerSize.SMALLINT);
  }

  @Override
  protected String getTableName() {
    return "viewing_location";
  }

  @Override
  public String toString() {
    return viewingLocationName.getValue();
  }


  @NotNull
  public static ViewingLocation findOrCreate(SQLConnection connection, String viewingLocationName) throws SQLException {
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
