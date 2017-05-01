package com.mayhew3.gamesutil.model.tv;

import com.mayhew3.gamesutil.dataobject.*;

public class TVDBUpdateError extends RetireableDataObject {

  public FieldValueForeignKey exceptionType = registerForeignKey(new TVDBUpdateErrorType(), Nullability.NOT_NULL);
  public FieldValueString exceptionMsg = registerStringField("exception_msg", Nullability.NOT_NULL);

  public FieldValueString context = registerStringField("context", Nullability.NOT_NULL);

  public FieldValueForeignKey seriesId = registerForeignKey(new Series(), Nullability.NULLABLE);
  public FieldValueInteger tvdbEpisodeExtId = registerIntegerField("tvdb_episode_ext_id", Nullability.NULLABLE);




  @Override
  public String getTableName() {
    return "tvdb_update_error";
  }
}
