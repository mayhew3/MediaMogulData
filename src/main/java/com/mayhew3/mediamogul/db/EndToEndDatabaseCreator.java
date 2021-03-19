package com.mayhew3.mediamogul.db;

import com.mayhew3.mediamogul.model.MediaMogulSchema;
import com.mayhew3.mediamogul.model.Person;
import com.mayhew3.mediamogul.model.tv.SystemVars;
import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.dataobject.DatabaseRecreator;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
import com.mayhew3.postgresobject.exception.MissingEnvException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.joda.time.DateTime;

import java.net.URISyntaxException;
import java.sql.SQLException;

public class EndToEndDatabaseCreator {
  private SQLConnection connection;

  private static Logger logger = LogManager.getLogger(EndToEndDatabaseCreator.class);

  private EndToEndDatabaseCreator(SQLConnection connection) {
    this.connection = connection;
  }

  public static void main(String... args) throws URISyntaxException, SQLException, MissingEnvException {
    ArgumentChecker argumentChecker = new ArgumentChecker(args);

    SQLConnection connection = PostgresConnectionFactory.createConnection(DatabaseEnvironments.getEnvironmentForDBArgument(argumentChecker));

    EndToEndDatabaseCreator endToEndDatabaseCreator = new EndToEndDatabaseCreator(connection);
    endToEndDatabaseCreator.runUpdate();
  }

  private void runUpdate() throws SQLException {
    logger.debug("Starting database recreate...");
    new DatabaseRecreator(connection).recreateDatabase(MediaMogulSchema.schema);
    logger.debug("Creating system data...");
    addSystemVars();
    logger.debug("Creating person data...");
    addPersonData();
    logger.debug("End-To-End database complete.");
  }

  @NotNull
  private DateTime endOfYear(Integer year) {
    return new DateTime(year, 12, 31, 0, 0, 0);
  }

  private void addSystemVars() throws SQLException {
    int year = 2019;

    SystemVars systemVars = new SystemVars();
    systemVars.initializeForInsert();
    systemVars.ratingYear.changeValue(year);
    systemVars.ratingEndDate.changeValue(endOfYear(year).toDate());
    systemVars.commit(connection);
  }

  private void addPersonData() throws SQLException {
    Person mayhew = new Person();
    mayhew.initializeForInsert();
    mayhew.email.changeValue("scorpy@gmail.com");
    mayhew.firstName.changeValue("Mayhew");
    mayhew.lastName.changeValue("Seavey");
    mayhew.userRole.changeValue("admin");
    mayhew.rating_notifications.changeValue(true);
    mayhew.commit(connection);

    Person teddy = new Person();
    teddy.initializeForInsert();
    teddy.email.changeValue("shanguinista.farbiton@gmail.com");
    teddy.firstName.changeValue("Teddy");
    teddy.lastName.changeValue("Ruxpin");
    teddy.userRole.changeValue("user");
    teddy.rating_notifications.changeValue(false);
    teddy.commit(connection);
  }
}
