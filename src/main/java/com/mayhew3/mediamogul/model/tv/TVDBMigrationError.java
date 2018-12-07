package com.mayhew3.mediamogul.model.tv;

import com.mayhew3.postgresobject.dataobject.*;

public class TVDBMigrationError extends RetireableDataObject {

  /* Foreign Keys */
  public FieldValueForeignKey seriesId = registerForeignKey(new Series(), Nullability.NOT_NULL);
  public FieldValueInteger tvdbEpisodeExtId = registerIntegerField("tvdb_episode_ext_id", Nullability.NULLABLE);

  public FieldValueString exceptionType = registerStringField("exception_type", Nullability.NOT_NULL);
  public FieldValueString exceptionMsg = registerStringField("exception_msg", Nullability.NOT_NULL);


  @Override
  public String getTableName() {
    return "tvdb_migration_error";
  }
}
