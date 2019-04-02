package com.mayhew3.mediamogul.model;

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

public class SchemaLocalTest {

  private static Logger logger = LogManager.getLogger(SchemaLocalTest.class);
  
  @Test
  public void testLocalSchemaUpToDate() throws URISyntaxException, SQLException {
    SQLConnection connection = PostgresConnectionFactory.getSqlConnection(PostgresConnectionFactory.LOCAL);
    List<DataObjectMismatch> mismatches = MediaMogulSchema.schema.validateSchemaAgainstDatabase(connection);

    if (!mismatches.isEmpty()) {
      debug("Mismatches found: ");
      for (DataObjectMismatch mismatch : mismatches) {
        debug(" - " + mismatch);
        if (mismatch.getMessage().equals("Table not found!")) {
          debug("    - " + mismatch.getDataObject().generateTableCreateStatement());
        }
        if (mismatch.getMessage().equals("ForeignKey restraint not found in DB.")) {
          List<String> stringList = mismatch.getDataObject().generateAddForeignKeyStatements();
          for (String fkStatement : stringList) {
            debug("    - " + fkStatement);
          }
        }
      }
      fail();
    }

  }

  private void debug(Object message) {
    logger.debug(message);
  }

}