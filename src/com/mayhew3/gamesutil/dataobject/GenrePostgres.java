package com.mayhew3.gamesutil.dataobject;

import com.mayhew3.gamesutil.db.SQLConnection;
import com.sun.istack.internal.NotNull;

import java.sql.ResultSet;
import java.sql.SQLException;

public class GenrePostgres extends DataObject {

  /* Data */
  public FieldValueString genreName = registerStringField("name");

  @Override
  protected String getTableName() {
    return "genre";
  }

  @Override
  public String toString() {
    return genreName.getValue();
  }

  @NotNull
  public static GenrePostgres findOrCreate(SQLConnection connection, String genreName) throws SQLException {
    GenrePostgres genrePostgres = new GenrePostgres();

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(
        "SELECT * FROM genre WHERE " + genrePostgres.genreName.getFieldName() + " = ?",
        genreName);

    if (resultSet.next()) {
      genrePostgres.initializeFromDBObject(resultSet);
    } else {
      genrePostgres.initializeForInsert();
      genrePostgres.genreName.changeValue(genreName);
      genrePostgres.commit(connection);
    }

    return genrePostgres;
  }
}