package com.mayhew3.mediamogul.scheduler;

import com.mayhew3.postgresobject.db.SQLConnection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

public abstract class TaskSchedule {
  protected SQLConnection connection;
  private UpdateRunner updateRunner;

  @Nullable
  Date lastRan = null;

  TaskSchedule(UpdateRunner updateRunner, SQLConnection connection) {
    this.updateRunner = updateRunner;
    this.connection = connection;
  }

  @NotNull
  UpdateRunner getUpdateRunner() {
    return updateRunner;
  }

  void updateLastRanToNow() {
    this.lastRan = new Date();
  }

  @NotNull
  public abstract Boolean isEligibleToRun();

  void updateLastRanFromDB() {
    try {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(
          "SELECT MAX(end_time) AS max_end_time " +
              "FROM connect_log " +
              "WHERE task_name = ? ",
          getUpdateRunner().getUniqueIdentifier());
      if (resultSet.next()) {
        Timestamp maxEndTime = resultSet.getTimestamp("max_end_time");
        if (maxEndTime != null) {
          lastRan = new Date(maxEndTime.getTime());
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

}
