package com.mayhew3.mediamogul.db;

import java.util.AbstractMap;
import java.util.Map;

public class ExecutionEnvironments {
  public static Map<String, ExecutionEnvironment> environments = Map.ofEntries(
      new AbstractMap.SimpleEntry<>("Obsidian", new ExecutionEnvironment("Obsidian", true, "backup")),
      new AbstractMap.SimpleEntry<>("Janet", new ExecutionEnvironment("Janet", true, "backup")),
      new AbstractMap.SimpleEntry<>("heroku", new ExecutionEnvironment("heroku", false, "updater"))
  );
}
