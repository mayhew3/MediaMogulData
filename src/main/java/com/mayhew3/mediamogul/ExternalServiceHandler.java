package com.mayhew3.mediamogul;

import com.google.common.annotations.VisibleForTesting;
import com.mayhew3.mediamogul.model.ExternalService;
import com.mayhew3.postgresobject.db.SQLConnection;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

public class ExternalServiceHandler {
  private SQLConnection connection;
  private ExternalService externalService;

  public ExternalServiceHandler(SQLConnection connection, ExternalServiceType serviceType) {
    this.connection = connection;
    String typekey = serviceType.getTypekey();

    try {
      this.externalService = ExternalService.getOrCreateExternalService(connection, typekey);
    } catch (SQLException e) {
      e.printStackTrace();
      throw new RuntimeException("Unable to get external service '" + typekey + "'");
    }
  }

  public void connectionFailed() {
    Date rightNow = new Date();
    if (shouldUpdateFailureTime(rightNow)) {
      externalService.last_failure.changeValue(rightNow);
      try {
        externalService.commit(connection);
      } catch (SQLException e) {
        e.printStackTrace();
        throw new RuntimeException("Unable to get update external service ID " + externalService.id.getValue());
      }
    }
  }

  public void connectionSuccess() {
    Date rightNow = new Date();
    if (shouldUpdateSuccessTime(rightNow)) {
      externalService.last_connect.changeValue(rightNow);
      try {
        externalService.commit(connection);
      } catch (SQLException e) {
        e.printStackTrace();
        throw new RuntimeException("Unable to get update external service ID " + externalService.id.getValue());
      }
    }
  }

  private boolean shouldUpdateSuccessTime(Date rightNow) {
    Timestamp lastConnect = externalService.last_connect.getValue();
    Timestamp lastFailure = externalService.last_failure.getValue();
    return lastConnect == null ||
        (lastFailure != null && lastFailure.getTime() > lastConnect.getTime()) ||
        rightNow.getTime() - lastConnect.getTime() > 60000;
  }

  private boolean shouldUpdateFailureTime(Date rightNow) {
    Timestamp lastConnect = externalService.last_connect.getValue();
    Timestamp lastFailure = externalService.last_failure.getValue();
    return lastFailure == null ||
        (lastConnect != null && lastConnect.getTime() > lastFailure.getTime()) ||
        rightNow.getTime() - lastFailure.getTime() > 60000;
  }
}
