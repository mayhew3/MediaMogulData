package com.mayhew3.mediamogul.db;

import java.util.AbstractMap;
import java.util.Map;

public class DatabaseEnvironments {
  public static Map<String, DatabaseEnvironment> environments = Map.ofEntries(
      new AbstractMap.SimpleEntry<>("local", new LocalDatabaseEnvironment("local", "tv", 11)),
      new AbstractMap.SimpleEntry<>("heroku", new HerokuDatabaseEnvironment("heroku", "postgresURL_heroku")),
      new AbstractMap.SimpleEntry<>("heroku-staging", new HerokuDatabaseEnvironment("heroku-staging", "postgresURL_heroku_staging"))
  );
}
