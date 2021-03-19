package com.mayhew3.mediamogul.tv;

import com.mashape.unirest.http.exceptions.UnirestException;
import com.mayhew3.mediamogul.ExternalServiceHandler;
import com.mayhew3.mediamogul.ExternalServiceType;
import com.mayhew3.mediamogul.db.ConnectionDetails;
import com.mayhew3.mediamogul.tv.provider.TVDBJWTProviderImpl;
import com.mayhew3.mediamogul.xml.JSONReaderImpl;
import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
import com.mayhew3.postgresobject.exception.MissingEnvException;
import org.apache.http.auth.AuthenticationException;

import java.net.URISyntaxException;
import java.sql.SQLException;

public class TVDBUpdateFinderRunner {

  public static void main(String... args) throws URISyntaxException, SQLException, MissingEnvException, UnirestException, AuthenticationException {
    ArgumentChecker argumentChecker = new ArgumentChecker(args);

    ConnectionDetails connectionDetails = ConnectionDetails.getConnectionDetails(argumentChecker);

    String dbUrl = connectionDetails.getDbUrl();
    SQLConnection connection = PostgresConnectionFactory.initiateDBConnect(dbUrl);
    ExternalServiceHandler tvdbServiceHandler = new ExternalServiceHandler(connection, ExternalServiceType.TVDB);

    TVDBJWTProviderImpl tvdbJWTProvider = new TVDBJWTProviderImpl(tvdbServiceHandler);
    JSONReaderImpl jsonReader = new JSONReaderImpl();

    TVDBUpdateFinder tvdbUpdateFinder = new TVDBUpdateFinder(connection, tvdbJWTProvider, jsonReader);
    tvdbUpdateFinder.runUpdate();
  }
}
