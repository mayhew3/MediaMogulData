package com.mayhew3.mediamogul;

import com.mayhew3.mediamogul.exception.MissingEnvException;
import com.mayhew3.mediamogul.model.MediaMogulSchema;
import com.mayhew3.postgresobject.dataobject.DatabaseRecreator;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;

import java.net.URISyntaxException;
import java.sql.SQLException;


public abstract class DatabaseTest {
  protected SQLConnection connection;

  private static Logger logger = LogManager.getLogger(DatabaseTest.class);

  @Before
  public void setUp() throws URISyntaxException, SQLException, MissingEnvException {
    logger.info("Setting up test DB...");
    connection = PostgresConnectionFactory.getSqlConnection(PostgresConnectionFactory.TEST);
    new DatabaseRecreator(connection).recreateDatabase(MediaMogulSchema.schema);
    logger.info("DB re-created.");
  }

  private void debug(Object message) {
    logger.debug(message);
  }

}
