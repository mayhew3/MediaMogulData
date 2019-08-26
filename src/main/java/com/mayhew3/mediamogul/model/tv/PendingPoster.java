package com.mayhew3.mediamogul.model.tv;

import com.mayhew3.postgresobject.dataobject.*;

public class PendingPoster extends RetireableDataObject {

  public PendingPoster() {
    FieldValueInteger tvdb_series_ext_id = registerIntegerField("tvdb_series_ext_id", Nullability.NOT_NULL);
    FieldValueString cloudinary_id = registerStringField("cloudinary_id", Nullability.NOT_NULL);
    registerStringField("tvdb_poster", Nullability.NULLABLE);

    addColumnsIndex(tvdb_series_ext_id, cloudinary_id, retired);
  }

  @Override
  public String getTableName() {
    return "pending_poster";
  }
}
