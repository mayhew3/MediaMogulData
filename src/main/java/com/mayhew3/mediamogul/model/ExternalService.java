package com.mayhew3.mediamogul.model;

import com.mayhew3.postgresobject.dataobject.DataObject;
import com.mayhew3.postgresobject.dataobject.FieldValueString;
import com.mayhew3.postgresobject.dataobject.FieldValueTimestamp;
import com.mayhew3.postgresobject.dataobject.Nullability;
import com.mayhew3.postgresobject.db.SQLConnection;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ExternalService extends DataObject {

  public FieldValueString service_name = registerStringField("service_name", Nullability.NOT_NULL);
  public FieldValueTimestamp last_connect = registerTimestampField("last_connect", Nullability.NULLABLE);
  public FieldValueTimestamp last_failure = registerTimestampField("last_failure", Nullability.NULLABLE);

  @Override
  public String getTableName() {
    return "external_service";
  }

  public static ExternalService getOrCreateExternalService(SQLConnection connection, String serviceName) throws SQLException {
    String sql = "SELECT * " +
        "FROM external_service " +
        "WHERE service_name = ? ";

    ExternalService externalService = new ExternalService();

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, serviceName);
    if (resultSet.next()) {
      externalService.initializeFromDBObject(resultSet);
    } else {
      externalService.initializeForInsert();
      externalService.service_name.changeValue(serviceName);
      externalService.commit(connection);
    }

    return externalService;
  }
}
