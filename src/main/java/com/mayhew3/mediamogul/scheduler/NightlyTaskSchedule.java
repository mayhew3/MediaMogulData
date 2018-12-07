package com.mayhew3.mediamogul.scheduler;

import com.mayhew3.postgresobject.db.SQLConnection;
import org.jetbrains.annotations.NotNull;
import org.joda.time.DateTime;
import org.joda.time.LocalTime;

public class NightlyTaskSchedule extends TaskSchedule {

  private LocalTime startTime;
  private LocalTime endTime;

  private Integer numberOfDays;

  NightlyTaskSchedule(UpdateRunner updateRunner, SQLConnection connection, Integer numberOfDays) {
    super(updateRunner, connection);
    this.startTime = new LocalTime(3, 0);
    this.endTime = new LocalTime(7, 0);
    this.numberOfDays = numberOfDays;
  }

  @NotNull
  @Override
  public Boolean isEligibleToRun() {
    return appropriateNumberOfDaysLater() && withinEligibleHours();
  }

  private boolean appropriateNumberOfDaysLater() {
    if (lastRan == null) {
      updateLastRanFromDB();
      if (lastRan == null) {
        return true;
      }
    }
    DateTime lastRanAtMidnight = new DateTime(lastRan).withTimeAtStartOfDay();
    DateTime now = new DateTime();
    DateTime nextEligible = lastRanAtMidnight.plusDays(numberOfDays);

    return !now.withTimeAtStartOfDay().isBefore(nextEligible);
  }

  private boolean withinEligibleHours() {
    DateTime now = new DateTime();
    return !now.toLocalTime().isBefore(startTime) && !now.toLocalTime().isAfter(endTime);
  }

}
