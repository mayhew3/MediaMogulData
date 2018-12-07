package com.mayhew3.mediamogul;

import com.mayhew3.mediamogul.model.MediaMogulSchema;
import com.mayhew3.postgresobject.dataobject.DatabaseRecreator;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
import org.junit.Before;

import java.net.URISyntaxException;
import java.sql.SQLException;


public abstract class DatabaseTest {
  protected SQLConnection connection;

  @Before
  public void setUp() throws URISyntaxException, SQLException {
    System.out.println("Setting up test DB...");
    connection = PostgresConnectionFactory.getSqlConnection(PostgresConnectionFactory.TEST);
    new DatabaseRecreator(connection).recreateDatabase(MediaMogulSchema.schema);
    System.out.println("DB re-created.");
  }

}
