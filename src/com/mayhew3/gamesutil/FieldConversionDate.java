package com.mayhew3.gamesutil;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class FieldConversionDate extends FieldConversion<Date> {
  @Override
  Date setValue(String value) {
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    try {
      return simpleDateFormat.parse(value);
    } catch (ParseException e) {
      e.printStackTrace();
      throw new RuntimeException("Unable to parse date " + value);
    }
  }
}
