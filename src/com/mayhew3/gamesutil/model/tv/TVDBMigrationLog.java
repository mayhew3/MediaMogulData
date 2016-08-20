package com.mayhew3.gamesutil.model.tv;

import com.mayhew3.gamesutil.dataobject.DataObject;
import com.mayhew3.gamesutil.dataobject.FieldValueForeignKey;
import com.mayhew3.gamesutil.dataobject.FieldValueString;
import com.mayhew3.gamesutil.dataobject.Nullability;

public class TVDBMigrationLog extends DataObject {

  /* Foreign Keys */
  public FieldValueForeignKey tvdbSeriesId = registerForeignKey(new TVDBSeries(), Nullability.NOT_NULL);
  public FieldValueForeignKey tvdbEpisodeId = registerForeignKey(new TVDBEpisode(), Nullability.NULLABLE);

  public FieldValueString tvdbFieldName = registerStringField("tvdb_field_name", Nullability.NOT_NULL);
  public FieldValueString oldValue = registerStringField("old_value", Nullability.NULLABLE);
  public FieldValueString newValue = registerStringField("new_value", Nullability.NULLABLE);


  @Override
  protected String getTableName() {
    return "tvdb_migration_log";
  }
}
