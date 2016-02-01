package com.mayhew3.gamesutil.mediaobject;

import com.mayhew3.gamesutil.mediaobject.FieldConversion;
import com.mayhew3.gamesutil.mediaobject.FieldValue;
import com.mongodb.BasicDBList;
import org.bson.types.ObjectId;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class FieldValueMongoArray extends FieldValue<BasicDBList> {
  public FieldValueMongoArray(String fieldName, FieldConversion<BasicDBList> converter) {
    super(fieldName, converter);
  }

  // todo: update code should really use $addToSet instead of sending the whole array in every time.
  public void addToArray(ObjectId value) {
    BasicDBList objectIds = getValue();
    if (objectIds == null || !objectIds.contains(value)) {
      BasicDBList dbList = new BasicDBList();
      if (objectIds != null) {
        dbList.addAll(objectIds);
      }
      dbList.add(value);
      changeValue(dbList);
    }
  }

  @Override
  protected void initializeValue(BasicDBList value) {
    super.initializeValue(value);
  }

  @Override
  protected void initializeValueFromString(String valueString) {
    super.initializeValueFromString(valueString);
  }

  @Override
  protected void initializeValue(ResultSet resultSet) {
    throw new IllegalStateException("Cannot select Postgres DB with Mongo value.");
  }

  @Override
  public void updatePreparedStatement(PreparedStatement preparedStatement, int currentIndex) {
    throw new IllegalStateException("Cannot update Postgres DB with Mongo value.");
  }

  public void removeFromArray(ObjectId value) {
    BasicDBList objectIds = getValue();
    BasicDBList dbList = new BasicDBList();
    dbList.addAll(objectIds);
    dbList.remove(value);
    changeValue(dbList);
  }
}
