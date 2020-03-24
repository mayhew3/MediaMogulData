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

  private void deleteValidView() throws SQLException {
    String sql = "DROP VIEW IF EXISTS valid_episode";
    connection.prepareAndExecuteStatementUpdate(sql);
  }

  private void deleteRegularView() throws SQLException {
    String sql = "DROP VIEW IF EXISTS regular_episode";
    connection.prepareAndExecuteStatementUpdate(sql);
  }

  private void deleteMatchingView() throws SQLException {
    String sql = "DROP VIEW IF EXISTS matched_series";
    connection.prepareAndExecuteStatementUpdate(sql);
  }

  private void deleteViews() throws SQLException {
    deleteMatchingView();
    deleteRegularView();
    deleteValidView();
  }

  private void createValidView() throws SQLException {
    String sql = "CREATE VIEW valid_episode " +
        "AS SELECT * " +
        "FROM episode " +
        "WHERE retired = 0 " +
        "AND tvdb_approval = 'approved' ";
    connection.prepareAndExecuteStatementUpdate(sql);
  }

  private void createRegularView() throws SQLException {
    String sql = "CREATE VIEW regular_episode " +
        "AS SELECT * " +
        "FROM valid_episode " +
        "WHERE season <> 0 ";
    connection.prepareAndExecuteStatementUpdate(sql);
  }

  private void createMatchingView() throws SQLException {
    String sql = "CREATE VIEW matched_series " +
        "AS SELECT * " +
        "FROM series " +
        "WHERE retired = 0 " +
        "AND tvdb_match_status = 'Match Completed' ";
    connection.prepareAndExecuteStatementUpdate(sql);
  }

  private void createViews() throws SQLException {
    createValidView();
    createRegularView();
    createMatchingView();
  }

  @Before
  public void setUp() throws URISyntaxException, SQLException, MissingEnvException {
    logger.info("Setting up test DB...");
    connection = PostgresConnectionFactory.getSqlConnection(PostgresConnectionFactory.TEST);
    deleteViews();
    new DatabaseRecreator(connection).recreateDatabase(MediaMogulSchema.schema);
    createViews();
    logger.info("DB re-created.");
  }

  private void debug(Object message) {
    logger.debug(message);
  }

}
