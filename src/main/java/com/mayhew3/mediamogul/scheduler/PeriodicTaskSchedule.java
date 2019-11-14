package com.mayhew3.mediamogul.scheduler;

import com.mayhew3.postgresobject.db.SQLConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.joda.time.DateTime;
import org.joda.time.Minutes;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

public class PeriodicTaskSchedule {
  protected SQLConnection connection;
  private UpdateRunner updateRunner;
  private Integer minutesBetween;

  @Nullable
  private Date lastRan = null;

  private static Logger logger = LogManager.getLogger(PeriodicTaskSchedule.class);

  PeriodicTaskSchedule(UpdateRunner updateRunner, SQLConnection connection) {
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

  PeriodicTaskSchedule withMinutesBetween(Integer minutesBetween) {
    this.minutesBetween = minutesBetween;
    return this;
  }

  PeriodicTaskSchedule withHoursBetween(Integer hoursBetween) {
    this.minutesBetween = hoursBetween * 60;
    return this;
  }

  Integer getMinutesBetween() {
    return minutesBetween;
  }

  @NotNull Boolean isEligibleToRun() {
    if (this.minutesBetween == null) {
      throw new IllegalStateException("Cannot run PeriodicTask before initializing the periodicity via withMinutesBetween() or withHoursBetween().");
    }
    if (lastRan == null) {
      updateLastRanFromDB();
      if (lastRan == null) {
        logger.info("Task '" + getUpdateRunner().getUniqueIdentifier() + "' has never been run! Running for first time.");
        return true;
      }
    }
    Minutes minutes = Minutes.minutesBetween(new DateTime(lastRan), new DateTime());
    boolean periodExceeded = minutes.getMinutes() >= minutesBetween;
    if (periodExceeded) {
      logger.info("Task '" + getUpdateRunner().getUniqueIdentifier() +
          "' is eligible to be run: Period of " + minutesBetween + " minutes, last run: " +
          lastRan + " (" + minutes.getMinutes() + " minutes ago)");
    }
    return periodExceeded;
  }

  private void updateLastRanFromDB() {
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

  Long getMillisUntilNextRun() {
    DateTime nextRun = new DateTime(lastRan).plusMinutes(minutesBetween);
    return nextRun.toDate().getTime() - new Date().getTime();
  }

  @Override
  public String toString() {
    return updateRunner.getUniqueIdentifier();
  }
}
