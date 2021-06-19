package com.mayhew3.mediamogul.onetimeutilities;

import com.mashape.unirest.http.exceptions.UnirestException;
import com.mayhew3.mediamogul.db.DatabaseEnvironments;
import com.mayhew3.mediamogul.games.provider.IGDBProvider;
import com.mayhew3.mediamogul.games.provider.IGDBProviderImpl;
import com.mayhew3.mediamogul.xml.JSONReader;
import com.mayhew3.mediamogul.xml.JSONReaderImpl;
import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.db.DatabaseEnvironment;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
import com.mayhew3.postgresobject.exception.MissingEnvException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URISyntaxException;
import java.sql.SQLException;

public class IGDBSteamLinker {

  private final SQLConnection connection;
  private final IGDBProvider igdbProvider;
  private final JSONReader jsonReader;

  private static final Logger logger = LogManager.getLogger(IGDBSteamLinker.class);

  public IGDBSteamLinker(SQLConnection connection, IGDBProvider igdbProvider, JSONReader jsonReader) {
    this.connection = connection;
    this.igdbProvider = igdbProvider;
    this.jsonReader = jsonReader;
  }

  public static void main(String... args) throws SQLException, MissingEnvException, URISyntaxException, UnirestException {
    ArgumentChecker argumentChecker = new ArgumentChecker(args);

    DatabaseEnvironment environment = DatabaseEnvironments.getEnvironmentForDBArgument(argumentChecker);
    SQLConnection connection = PostgresConnectionFactory.createConnection(environment);

    IGDBSteamLinker igdbSteamLinker = new IGDBSteamLinker(connection, new IGDBProviderImpl(), new JSONReaderImpl());
    igdbSteamLinker.runUpdate();
  }

  private void runUpdate() {
    String sql = "SELECT * " +
        "FROM game " +
        "WHERE steamid IS NULL ";

  }
}