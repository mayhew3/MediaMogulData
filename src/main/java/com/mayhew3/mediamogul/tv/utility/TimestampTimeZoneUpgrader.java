package com.mayhew3.mediamogul.tv.utility;

import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.dataobject.*;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
import com.mayhew3.mediamogul.model.MediaMogulSchema;

import java.net.URISyntaxException;
import java.sql.SQLException;

public class TimestampTimeZoneUpgrader {
  private DataSchema schema;
  private SQLConnection connection;

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
    debug("Updating {" + table.getTableName() + ", " + fieldValue.getFieldName() + "}");

    String sql =
        "ALTER TABLE " + table.getTableName() + " " +
        "ALTER COLUMN " + fieldValue.getFieldName() + " " +
        "TYPE timestamp with time zone " +
        "USING " + fieldValue.getFieldName() + " AT TIME ZONE 'America/Los_Angeles'";
    connection.prepareAndExecuteStatementUpdate(sql);
  }


  protected void debug(Object object) {
    System.out.println(object);
  }

}
