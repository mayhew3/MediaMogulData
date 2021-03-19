package com.mayhew3.mediamogul.archive;

import com.mayhew3.postgresobject.db.DatabaseEnvironment;
import com.mayhew3.mediamogul.db.DatabaseEnvironments;
import com.mayhew3.mediamogul.scheduler.UpdateRunner;
import com.mayhew3.mediamogul.tv.helper.UpdateMode;
import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.EnvironmentChecker;
import com.mayhew3.postgresobject.db.*;
import com.mayhew3.postgresobject.exception.MissingEnvException;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("rawtypes")
public class OldDataArchiveRunner implements UpdateRunner {

  private final DataArchiver dataArchiver;

  public static void main(String... args) throws URISyntaxException, SQLException, IOException, MissingEnvException {
    ArgumentChecker argumentChecker = new ArgumentChecker(args);

    DatabaseEnvironment environment = DatabaseEnvironments.getEnvironmentForDBArgument(argumentChecker);
    SQLConnection connection = PostgresConnectionFactory.createConnection(environment);
    String dbIdentifier = argumentChecker.getDBIdentifier();

    OldDataArchiveRunner runner = new OldDataArchiveRunner(connection, dbIdentifier);
    runner.runUpdate();
  }

  public OldDataArchiveRunner(SQLConnection connection, String dbIdentifier) throws MissingEnvException {
    String logDirectory = EnvironmentChecker.getOrThrow("MediaMogulArchives");

    List<ArchiveableFactory> tablesToArchive = new ArrayList<>();
    tablesToArchive.add(new ConnectLogFactory());
    tablesToArchive.add(new TVDBMigrationLogFactory());
    tablesToArchive.add(new TVDBConnectionLogFactory());

    dataArchiver = new DataArchiver(connection, dbIdentifier, logDirectory, tablesToArchive);
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
    dataArchiver.runUpdate();
  }

}
