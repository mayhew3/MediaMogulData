package com.mayhew3.mediamogul.model.tv.group;

import com.mayhew3.mediamogul.model.Person;
import com.mayhew3.postgresobject.dataobject.*;

public class TVGroupBallot extends RetireableDataObject {

  public FieldValueTimestamp voting_open = registerTimestampField("voting_open",Nullability.NOT_NULL).defaultValueNow();
  public FieldValueString reason = registerStringField("reason", Nullability.NULLABLE);
  public FieldValueForeignKey tv_group_series = registerForeignKey(new TVGroupSeries(), Nullability.NOT_NULL);

  public TVGroupBallot() {
    registerTimestampField("voting_closed", Nullability.NULLABLE);
    registerIntegerField("last_episode", Nullability.NULLABLE);
    registerIntegerField("season", Nullability.NULLABLE);
    registerIntegerField("first_episode", Nullability.NULLABLE);
    registerBooleanField("skip", Nullability.NOT_NULL).defaultValue(false);
    registerForeignKey(new Person(), Nullability.NULLABLE);
  }

  @Override
  public String getTableName() {
    return "tv_group_ballot";
  }
}
