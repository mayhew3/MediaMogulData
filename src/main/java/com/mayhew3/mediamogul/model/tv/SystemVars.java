package com.mayhew3.mediamogul.model.tv;

import com.mayhew3.postgresobject.dataobject.*;
import com.mayhew3.postgresobject.db.SQLConnection;

import java.sql.ResultSet;
import java.sql.SQLException;

public class SystemVars extends DataObject {

  public FieldValueInteger ratingYear = registerIntegerField("rating_year", Nullability.NOT_NULL);
  public FieldValueTimestamp ratingEndDate = registerTimestampField("rating_end_date", Nullability.NULLABLE);

  @Override
  public String getTableName() {
    return "system_vars";
  }

  public static SystemVars getSystemVars(SQLConnection connection) throws SQLException {
    String sql = "SELECT * " +
        "FROM system_vars";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql);
    if (resultSet.next()) {
      SystemVars systemVars = new SystemVars();
      systemVars.initializeFromDBObject(resultSet);

      return systemVars;
    } else {
      throw new IllegalStateException("No rows found in system_vars.");
    }
  }
}
