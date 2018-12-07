package com.mayhew3.mediamogul.model.tv;

import com.mayhew3.postgresobject.dataobject.DataObject;
import com.mayhew3.postgresobject.dataobject.FieldValueString;
import com.mayhew3.postgresobject.dataobject.Nullability;
import com.mayhew3.postgresobject.db.SQLConnection;
import org.jetbrains.annotations.NotNull;

import java.sql.ResultSet;
import java.sql.SQLException;

public class Genre extends DataObject {

  /* Data */
  public FieldValueString genreName = registerStringField("name", Nullability.NOT_NULL);

  @Override
  public String getTableName() {
    return "genre";
  }

  @Override
  public String toString() {
    return genreName.getValue();
  }

  @NotNull
  static Genre findOrCreate(SQLConnection connection, String genreName) throws SQLException {
    Genre genre = new Genre();

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(
        "SELECT * FROM genre WHERE " + genre.genreName.getFieldName() + " = ?",
        genreName);

    if (resultSet.next()) {
      genre.initializeFromDBObject(resultSet);
    } else {
      genre.initializeForInsert();
      genre.genreName.changeValue(genreName);
      genre.commit(connection);
    }

    return genre;
  }
}
