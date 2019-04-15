package com.mayhew3.mediamogul.archive;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.mayhew3.mediamogul.EnvironmentChecker;
import com.mayhew3.mediamogul.exception.MissingEnvException;
import com.mayhew3.mediamogul.scheduler.UpdateRunner;
import com.mayhew3.mediamogul.tv.helper.UpdateMode;
import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.dataobject.DataObject;
import com.mayhew3.postgresobject.dataobject.FieldValue;
import com.mayhew3.postgresobject.dataobject.FieldValueTimestamp;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.joda.time.DateTime;
import org.joda.time.DateTimeComparator;

import java.io.*;
import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class OldDataArchiveRunner implements UpdateRunner {

  private SQLConnection connection;
  private String dbIdentifier;

  // Map of DB table to months of data to keep.
  private List<ArchiveableFactory> tablesToArchive;
  private String mediaMogulLogs;

  private static Logger logger = LogManager.getLogger(OldDataArchiveRunner.class);

  public static void main(String... args) throws URISyntaxException, SQLException, IOException, MissingEnvException {
    ArgumentChecker argumentChecker = new ArgumentChecker(args);

    SQLConnection connection = PostgresConnectionFactory.createConnection(argumentChecker);
    String dbIdentifier = argumentChecker.getDBIdentifier();

    OldDataArchiveRunner runner = new OldDataArchiveRunner(connection, dbIdentifier);
    runner.runUpdate();
  }


  public OldDataArchiveRunner(SQLConnection connection, String dbIdentifier) throws MissingEnvException {
    this.connection = connection;
    this.dbIdentifier = dbIdentifier;
    this.mediaMogulLogs = EnvironmentChecker.getOrThrow("MediaMogulArchives");
    tablesToArchive = new ArrayList<>();

    // ADD NEW TABLES TO ARCHIVE HERE!
    tablesToArchive.add(new ConnectLogFactory());
    tablesToArchive.add(new TVDBMigrationLogFactory());
    tablesToArchive.add(new TVDBConnectionLogFactory());
  }

  @Override
  public String getRunnerName() {
    return "Old Data Archive";
  }

  @Override
  public @Nullable UpdateMode getUpdateMode() {
    return null;
  }

  @Override
  public void runUpdate() throws SQLException, IOException {
    for (ArchiveableFactory factory : tablesToArchive) {
      runUpdateOnTable(factory);
    }
  }

  private void runUpdateOnTable(ArchiveableFactory factory) throws SQLException, IOException {
    String tableName = factory.tableName();
    String dateColumnName = factory.dateColumnName();
    Integer monthsToKeep = factory.monthsToKeep();

    DateTime today = new DateTime();
    DateTime lastDateToKeep = today.minusMonths(monthsToKeep);
    Timestamp lastDateInTimestamp = new Timestamp(lastDateToKeep.toDate().getTime());

    File mostRecentFile;
    DateTime mostRecentDate = null;
    PrintStream mostRecentStream = null;

    String sql = "SELECT * " +
        " FROM " + tableName +
        " WHERE " + dateColumnName + " IS NOT NULL " +
        " AND " + dateColumnName + " < ? " +
        " ORDER BY " + dateColumnName;

    Integer i = 0;

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, lastDateInTimestamp);

    logger.info("Query executed on table " + tableName + " before " + lastDateToKeep + ". Processing rows...");


    while (resultSet.next()) {
      DataObject dataObject = factory.createEntity();
      dataObject.initializeFromDBObject(resultSet);

      FieldValueTimestamp dateValue = (FieldValueTimestamp) dataObject.getFieldValueWithName(dateColumnName);
      assert dateValue != null;

      Timestamp rowTimestamp = dateValue.getValue();
      DateTime rowDateTime = new DateTime(rowTimestamp);

      if (mostRecentDate == null || DateTimeComparator.getDateOnlyInstance().compare(mostRecentDate, rowDateTime) < 0) {
        mostRecentDate = rowDateTime;
        mostRecentFile = getFile(tableName, rowTimestamp);

        if (mostRecentFile.exists()) {
          BufferedReader bufferedReader = new BufferedReader(new FileReader(mostRecentFile));
          String firstLine = bufferedReader.readLine();
          if (firstLine != null) {
            validateHeaderRow(dataObject, firstLine);
          }
          mostRecentStream = new PrintStream(new FileOutputStream(mostRecentFile, true));
        } else {
          List<String> fieldNames = getFieldNames(dataObject);
          String headerRow = Joiner.on(",").join(fieldNames);
          mostRecentStream = new PrintStream(new FileOutputStream(mostRecentFile, true));
          mostRecentStream.println(headerRow);
        }
      }

      // todo: Check for duplicate
      copyRowToArchiveFile(dataObject, mostRecentStream);
      deleteOldData(factory, dataObject.id.getValue());
      i++;

      if (i % 100 == 0) {
        debug(i + " rows processed.");
      }
    }

    logger.info(i + " rows processed. Done with table " + tableName);
  }

  private void debug(String message) {
    logger.debug(message);
  }

  private void copyRowToArchiveFile(DataObject dataObject, @NotNull PrintStream printStream) {
    List<String> values = dataObject.getAllFieldValuesIncludingId().stream()
        .sorted(Comparator.comparing(FieldValue::getFieldName))
        .map(fieldValue -> fieldValue.getValue() == null ? "" : fieldValue.getValue().toString())
        .collect(Collectors.toList());

    String valueText = Joiner.on(",").join(values);

    printStream.println(valueText);
  }

  private void validateHeaderRow(DataObject dataObject, String existingHeader) {
    List<String> headerValues = Lists.newArrayList(existingHeader.split(","));
    List<String> expectedValues = getFieldNames(dataObject);

    if (!expectedValues.equals(headerValues)) {
      throw new RuntimeException("Header row on existing file doesn't match data model of " + dataObject.getTableName() + ". " +
          "Expected: " + expectedValues + ", " +
          "Actual: " + headerValues);
    }
  }

  private List<String> getFieldNames(DataObject dataObject) {
    return dataObject.getAllFieldValuesIncludingId()
        .stream()
        .map(FieldValue::getFieldName)
        .sorted()
        .collect(Collectors.toList());
  }

  private void deleteOldData(ArchiveableFactory factory, Integer rowId) throws SQLException {
    String sql = "DELETE FROM " + factory.tableName() +
        " WHERE ID = ? ";
    connection.prepareAndExecuteStatementUpdate(sql, rowId);
  }

  @NotNull
  private File getFile(String tableName, Timestamp rowDate) {
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
    String dateFormatted = simpleDateFormat.format(rowDate);

    File directory = new File(mediaMogulLogs + "\\" + dbIdentifier);
    if (!directory.exists()) {
      //noinspection ResultOfMethodCallIgnored
      directory.mkdir();
    }

    return new File(mediaMogulLogs + "\\" + dbIdentifier + "\\Archive_" + tableName + "_" + dateFormatted + ".csv");
  }

}
