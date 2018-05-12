package com.mayhew3.mediamogul.archive;

import com.mayhew3.mediamogul.model.tv.TVDBConnectionLog;

public class TVDBConnectionLogFactory extends ArchiveableFactory<TVDBConnectionLog> {
  @Override
  public TVDBConnectionLog createEntity() {
    return new TVDBConnectionLog();
  }

  @Override
  public Integer monthsToKeep() {
    return 6;
  }

  @Override
  public String tableName() {
    return "tvdb_connection_log";
  }

  @Override
  public String dateColumnName() {
    return "start_time";
  }
}
