package com.mayhew3.mediamogul;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class DateUtils {
  private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");

  /**
   * Convert {@link LocalDate} to {@link DateTime}
   */
  public static DateTime toDateTime(LocalDate localDate) {
    return new DateTime(DateTimeZone.UTC).withDate(
        localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth()
    ).withTime(0, 0, 0, 0);
  }

  public static Date toDate(LocalDate localDate) {
    return Date.from(localDate.atStartOfDay(ZoneId.systemDefault()).toInstant());
  }

  public static String formatDate(LocalDate localDate) {
    return localDate.format(formatter);
  }

  public static LocalDate parseDate(String dateStr) {
    return LocalDate.parse(dateStr, formatter);
  }

  public static boolean betweenInclusive(LocalDate start, LocalDate end, LocalDate dateToTest) {
    return !dateToTest.isBefore(start) && !dateToTest.isAfter(end);
  }

  public static LocalDate fromTimestamp(Timestamp timestamp) {
    return timestamp.toLocalDateTime().toLocalDate();
  }

  public static LocalDateTime localTimeFromTimestamp(Timestamp timestamp) {
    return timestamp.toLocalDateTime();
  }

  /**
   * Convert {@link DateTime} to {@link LocalDate}
   */
  public static LocalDate toLocalDate(DateTime dateTime) {
    DateTime dateTimeUtc = dateTime.withZone(DateTimeZone.UTC);
    return LocalDate.of(dateTimeUtc.getYear(), dateTimeUtc.getMonthOfYear(), dateTimeUtc.getDayOfMonth());
  }
}