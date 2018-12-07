package com.mayhew3.mediamogul.archive;

import com.mayhew3.postgresobject.dataobject.DataObject;

public abstract class ArchiveableFactory<T extends DataObject> {
  public abstract T createEntity();
  public abstract Integer monthsToKeep();
  public abstract String tableName();
  public abstract String dateColumnName();
}
