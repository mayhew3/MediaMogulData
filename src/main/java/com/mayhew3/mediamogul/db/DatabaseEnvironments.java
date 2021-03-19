package com.mayhew3.mediamogul.db;

import java.util.AbstractMap;
import java.util.Map;

public class DatabaseEnvironments {
  public static Map<String, DatabaseEnvironment> environments = Map.ofEntries(
      new AbstractMap.SimpleEntry<>("local", new LocalDatabaseEnvironment("local", "tv", 11)),
      new AbstractMap.SimpleEntry<>("local_13", new LocalDatabaseEnvironment("local_13", "tv", 13)),
      new AbstractMap.SimpleEntry<>("test", new LocalDatabaseEnvironment("test", "tv_copy", 11)),
      new AbstractMap.SimpleEntry<>("e2e", new LocalDatabaseEnvironment("e2e", "tv_e2e", 11)),
      new AbstractMap.SimpleEntry<>("heroku", new HerokuDatabaseEnvironment("heroku", "postgresURL_heroku", 11, "media-mogul")),
      new AbstractMap.SimpleEntry<>("heroku-staging", new HerokuDatabaseEnvironment("heroku-staging", "postgresURL_heroku_staging", 11, "media-mogul-staging"))
  );
}
