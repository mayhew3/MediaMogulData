package com.mayhew3.mediamogul.scheduler;

import com.mayhew3.postgresobject.db.SQLConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.joda.time.DateTime;
import org.joda.time.Minutes;

public class PeriodicTaskSchedule extends TaskSchedule {
  private Integer minutesBetween;

  private static Logger logger = LogManager.getLogger(PeriodicTaskSchedule.class);

  PeriodicTaskSchedule(UpdateRunner updateRunner, SQLConnection connection) {
    super(updateRunner, connection);
  }

  PeriodicTaskSchedule withMinutesBetween(Integer minutesBetween) {
    this.minutesBetween = minutesBetween;
    return this;
  }

  PeriodicTaskSchedule withHoursBetween(Integer hoursBetween) {
    this.minutesBetween = hoursBetween * 60;
    return this;
  }

  @NotNull
  @Override
  public Boolean isEligibleToRun() {
    if (this.minutesBetween == null) {
      throw new IllegalStateException("Cannot run PeriodicTask before initializing the periodicity via withMinutesBetween() or withHoursBetween().");
    }
    if (lastRan == null) {
      updateLastRanFromDB();
      if (lastRan == null) {
        logger.info("Task has never been run! Running for first time.");
        return true;
      }
    }
    Minutes minutes = Minutes.minutesBetween(new DateTime(lastRan), new DateTime());
    boolean periodExceeded = minutes.getMinutes() > minutesBetween;
    if (periodExceeded) {
      logger.info("Task '" + getUpdateRunner().getUniqueIdentifier() +
          "' is eligible to be run: Period of " + minutesBetween + " minutes, last run: " +
          lastRan + " (" + minutes.getMinutes() + " minutes ago)");
    }
    return periodExceeded;
  }
}
