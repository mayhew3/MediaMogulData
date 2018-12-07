package com.mayhew3.mediamogul.model.tv;

import com.mayhew3.postgresobject.dataobject.FieldValueForeignKey;
import com.mayhew3.postgresobject.dataobject.FieldValueString;
import com.mayhew3.postgresobject.dataobject.Nullability;
import com.mayhew3.postgresobject.dataobject.RetireableDataObject;

public class TVDBMigrationLog extends RetireableDataObject {

  /* Foreign Keys */
  public FieldValueForeignKey tvdbSeriesId = registerForeignKey(new TVDBSeries(), Nullability.NOT_NULL);
  public FieldValueForeignKey tvdbEpisodeId = registerForeignKey(new TVDBEpisode(), Nullability.NULLABLE);

  public FieldValueString tvdbFieldName = registerStringField("tvdb_field_name", Nullability.NOT_NULL);
  public FieldValueString oldValue = registerStringField("old_value", Nullability.NULLABLE);
  public FieldValueString newValue = registerStringField("new_value", Nullability.NULLABLE);


  @Override
  public String getTableName() {
    return "tvdb_migration_log";
  }
}
