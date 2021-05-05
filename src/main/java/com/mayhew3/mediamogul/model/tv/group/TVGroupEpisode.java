package com.mayhew3.mediamogul.model.tv.group;

import com.mayhew3.mediamogul.model.tv.Episode;
import com.mayhew3.postgresobject.dataobject.FieldValue;
import com.mayhew3.postgresobject.dataobject.FieldValueForeignKey;
import com.mayhew3.postgresobject.dataobject.Nullability;
import com.mayhew3.postgresobject.dataobject.RetireableDataObject;

public class TVGroupEpisode extends RetireableDataObject {

  /* Data */
  public FieldValueForeignKey tv_group_id = registerForeignKey(new TVGroup(), Nullability.NOT_NULL);
  public FieldValueForeignKey episode_id = registerForeignKey(new Episode(), Nullability.NOT_NULL);


  public TVGroupEpisode() {
    super();
    registerBooleanField("watched", Nullability.NOT_NULL);
    registerTimestampField("watched_date", Nullability.NULLABLE);

    registerBooleanField("skipped", Nullability.NOT_NULL);
    registerStringField("skip_reason", Nullability.NULLABLE);

    addUniqueConstraint(tv_group_id, episode_id);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public TVGroupEpisode createUncommittedCopy() {
    TVGroupEpisode tvGroupEpisode = new TVGroupEpisode();
    tvGroupEpisode.initializeForInsert();
    for (FieldValue fieldValue : getAllFieldValues()) {
      FieldValue newFieldValue = tvGroupEpisode.getFieldValueWithName(fieldValue.getFieldName());
      assert newFieldValue != null;
      newFieldValue.changeValue(fieldValue.getValue());
    }
    return tvGroupEpisode;
  }

  @Override
  public String getTableName() {
    return "tv_group_episode";
  }

  @Override
  public String toString() {
    return "tv_group_episode " + tv_group_id.getValue() + ", " + episode_id.getValue();
  }

}
