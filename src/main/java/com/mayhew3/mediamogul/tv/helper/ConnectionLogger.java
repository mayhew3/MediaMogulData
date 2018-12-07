package com.mayhew3.mediamogul.tv.helper;

import com.mayhew3.mediamogul.model.tv.ConnectLog;
import com.mayhew3.postgresobject.db.SQLConnection;
import com.mayhew3.mediamogul.scheduler.UpdateRunner;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Optional;

public class ConnectionLogger {

  private SQLConnection connection;
  private Integer connectionID;

  public ConnectionLogger(SQLConnection connection) throws SQLException {
    this.connection = connection;
    ConnectLog lastConnectLog = findMostRecentLog().orElse(null);
    connectionID = (lastConnectLog == null ? 0 : lastConnectLog.connectionID.getValue()) + 1;
  }

  public void close() {
    connection = null;
    connectionID = null;
  }

  private Optional<ConnectLog> findMostRecentLog() throws SQLException {
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch("SELECT * FROM connect_log ORDER BY id DESC");

    if (resultSet.next()) {
      ConnectLog connectLog = new ConnectLog();
      connectLog.initializeFromDBObject(resultSet);
      return Optional.of(connectLog);
    }

    return Optional.empty();
  }

  @Deprecated
  public void logConnectionStart(Boolean lookAtAllShows) throws SQLException {
    if (connectionID == null) {
      throw new IllegalStateException("No connection id on start.");
    }

    ConnectLog connectLog = new ConnectLog();
    connectLog.initializeForInsert();

    connectLog.startTime.changeValue(new Date());
    connectLog.connectionID.changeValue(connectionID);
    connectLog.fastUpdate.changeValue(!lookAtAllShows);

    connectLog.commit(connection);
  }

  public void logConnectionStart(UpdateRunner updateRunner) throws SQLException {
    if (connectionID == null) {
      throw new IllegalStateException("No connection id on start.");
    }

    ConnectLog connectLog = new ConnectLog();
    connectLog.initializeForInsert();

    connectLog.startTime.changeValue(new Date());
    connectLog.connectionID.changeValue(connectionID);
    connectLog.fastUpdate.changeValue(false);
    connectLog.taskName.changeValue(updateRunner.getUniqueIdentifier());

    UpdateMode updateMode = updateRunner.getUpdateMode();
    connectLog.taskMode.changeValue(updateMode == null ? null : updateMode.getTypekey());

    connectLog.commit(connection);
  }

  public void logConnectionEnd() throws SQLException {
    if (connectionID == null) {
      throw new IllegalStateException("Cannot have connection end that has no start.");
    }

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch("SELECT * FROM connect_log WHERE connection_id = ?", connectionID);

    if (resultSet.next()) {
      ConnectLog connectLog = new ConnectLog();
      connectLog.initializeFromDBObject(resultSet);

      Timestamp startTime = connectLog.startTime.getValue();
      Timestamp endTime = new Timestamp(new Date().getTime());

      long diffInMillis = endTime.getTime() - startTime.getTime();
      long diffInSeconds = diffInMillis / 1000;

      connectLog.endTime.changeValue(endTime);
      connectLog.timeConnected.changeValue(diffInSeconds);
      connectLog.commit(connection);

      close();
    } else {
      throw new RuntimeException("Unable to find connect log with ID " + connectionID);
    }

  }
}
