package com.mayhew3.mediamogul.model;

import com.mayhew3.mediamogul.EnvironmentChecker;
import com.mayhew3.mediamogul.exception.MissingEnvException;
import com.mayhew3.postgresobject.dataobject.DataObjectMismatch;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.List;

import static org.junit.Assert.fail;

public class SchemaHerokuTest {
  
  private static Logger logger = LogManager.getLogger(SchemaHerokuTest.class);
  
  @Test
  public void testHerokuSchemaUpToDate() throws URISyntaxException, SQLException, MissingEnvException {
    String database_url = EnvironmentChecker.getOrThrow("DATABASE_URL");
    logger.debug("Database URL: " + database_url);
    SQLConnection connection = PostgresConnectionFactory.initiateDBConnect(database_url);
    List<DataObjectMismatch> mismatches = MediaMogulSchema.schema.validateSchemaAgainstDatabase(connection);

    if (!mismatches.isEmpty()) {
      logger.error("Mismatches found: ");
      for (DataObjectMismatch mismatch : mismatches) {
        logger.error(" - " + mismatch);
        if (mismatch.getMessage().equals("Table not found!")) {
          logger.error("    - " + mismatch.getDataObject().generateTableCreateStatement());
        }
        if (mismatch.getMessage().equals("ForeignKey restraint not found in DB.")) {
          List<String> stringList = mismatch.getDataObject().generateAddForeignKeyStatements();
          for (String fkStatement : stringList) {
            logger.error("    - " + fkStatement);
          }
        }
      }
      fail();
    }

  }

}