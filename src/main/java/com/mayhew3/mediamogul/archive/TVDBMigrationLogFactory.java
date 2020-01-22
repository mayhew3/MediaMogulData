package com.mayhew3.mediamogul.archive;

import com.mayhew3.mediamogul.model.tv.TVDBMigrationLog;
import com.mayhew3.postgresobject.db.ArchiveableFactory;

public class TVDBMigrationLogFactory extends ArchiveableFactory<TVDBMigrationLog> {
  @Override
  public TVDBMigrationLog createEntity() {
    return new TVDBMigrationLog();
  }

  @Override
  public Integer monthsToKeep() {
    return 6;
  }

  @Override
  public String tableName() {
    return "tvdb_migration_log";
  }

  @Override
  public String dateColumnName() {
    return "date_added";
  }

  @Override
  public String otherColumnName() {
    return null;
  }

  @Override
  public Object otherColumnValue() {
    return null;
  }
}
