package com.mayhew3.mediamogul.tv.utility;

import com.mayhew3.mediamogul.model.MediaMogulSchema;
import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.dataobject.DataObject;
import com.mayhew3.postgresobject.dataobject.DataSchema;
import com.mayhew3.postgresobject.dataobject.FieldValue;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URISyntaxException;
import java.sql.SQLException;

public class TimestampTimeZoneUpgrader {
  private DataSchema schema;
  private SQLConnection connection;

  private static Logger logger = LogManager.getLogger(TimestampTimeZoneUpgrader.class);

  private TimestampTimeZoneUpgrader(DataSchema schema, SQLConnection connection) {
    this.schema = schema;
    this.connection = connection;
  }

  public static void main(String... args) throws URISyntaxException, SQLException {
    ArgumentChecker argumentChecker = new ArgumentChecker(args);
    SQLConnection connection = PostgresConnectionFactory.createConnection(argumentChecker);

    TimestampTimeZoneUpgrader upgrader = new TimestampTimeZoneUpgrader(MediaMogulSchema.schema, connection);
    upgrader.upgradeColumns();
  }

  private void upgradeColumns() throws SQLException {
    for (DataObject table : schema.getAllTables()) {
      for (FieldValue fieldValue : table.getAllFieldValues()) {
        if ("timestamp without time zone".equalsIgnoreCase(fieldValue.getInformationSchemaType()) ||
            "date_added".equalsIgnoreCase(fieldValue.getFieldName())) {
          upgradeColumn(fieldValue, table);
        }
      }
    }
  }

  private void upgradeColumn(FieldValue fieldValue, DataObject table) throws SQLException {
    logger.info("Updating {" + table.getTableName() + ", " + fieldValue.getFieldName() + "}");

    String sql =
        "ALTER TABLE " + table.getTableName() + " " +
        "ALTER COLUMN " + fieldValue.getFieldName() + " " +
        "TYPE timestamp with time zone " +
        "USING " + fieldValue.getFieldName() + " AT TIME ZONE 'America/Los_Angeles'";
    connection.prepareAndExecuteStatementUpdate(sql);
  }


  private void debug(Object message) {
    logger.debug(message);
  }

}
