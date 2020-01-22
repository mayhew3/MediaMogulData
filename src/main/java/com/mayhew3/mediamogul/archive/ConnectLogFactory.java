package com.mayhew3.mediamogul.archive;

import com.mayhew3.mediamogul.model.tv.ConnectLog;
import com.mayhew3.postgresobject.db.ArchiveableFactory;

public class ConnectLogFactory extends ArchiveableFactory<ConnectLog> {
  @Override
  public ConnectLog createEntity() {
    return new ConnectLog();
  }

  @Override
  public Integer monthsToKeep() {
    return 3;
  }

  @Override
  public String tableName() {
    return "connect_log";
  }

  @Override
  public String dateColumnName() {
    return "start_time";
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
