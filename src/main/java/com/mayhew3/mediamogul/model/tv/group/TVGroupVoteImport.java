package com.mayhew3.mediamogul.model.tv.group;

import com.mayhew3.postgresobject.dataobject.*;

public class TVGroupVoteImport extends DataObject {

  public final FieldValueString show = registerStringField("show", Nullability.NOT_NULL);
  public final FieldValueString email = registerStringField("email", Nullability.NOT_NULL);
  public final FieldValueTimestamp vote_date = registerTimestampField("vote_date", Nullability.NOT_NULL);
  public final FieldValueInteger vote = registerIntegerField("vote", Nullability.NOT_NULL);
  public final FieldValueBoolean imported = registerBooleanField("imported", Nullability.NOT_NULL).defaultValue(false);

  public TVGroupVoteImport() {
    addUniqueConstraint(show, email, vote_date);
  }

  @Override
  public String getTableName() {
    return "tv_group_vote_import";
  }
}
