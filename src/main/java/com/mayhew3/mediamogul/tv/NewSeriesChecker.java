package com.mayhew3.mediamogul.tv;

import com.cloudinary.Singleton;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.mayhew3.mediamogul.ArgumentChecker;
import com.mayhew3.mediamogul.ExternalServiceHandler;
import com.mayhew3.mediamogul.ExternalServiceType;
import com.mayhew3.mediamogul.db.ConnectionDetails;
import com.mayhew3.mediamogul.exception.MissingEnvException;
import com.mayhew3.mediamogul.model.tv.Series;
import com.mayhew3.mediamogul.scheduler.UpdateRunner;
import com.mayhew3.mediamogul.tv.exception.ShowFailedException;
import com.mayhew3.mediamogul.tv.helper.MetacriticException;
import com.mayhew3.mediamogul.tv.helper.UpdateMode;
import com.mayhew3.mediamogul.tv.provider.TVDBJWTProvider;
import com.mayhew3.mediamogul.tv.provider.TVDBJWTProviderImpl;
import com.mayhew3.mediamogul.xml.JSONReader;
import com.mayhew3.mediamogul.xml.JSONReaderImpl;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
import org.apache.http.auth.AuthenticationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;

public class NewSeriesChecker implements UpdateRunner {

  private SQLConnection connection;
  private TVDBJWTProvider tvdbjwtProvider;
  private JSONReader jsonReader;

  private static Logger logger = LogManager.getLogger(NewSeriesChecker.class);

  public NewSeriesChecker(SQLConnection connection, TVDBJWTProvider tvdbjwtProvider, JSONReader jsonReader) {
    this.connection = connection;
    this.tvdbjwtProvider = tvdbjwtProvider;
    this.jsonReader = jsonReader;
  }

  public static void main(String... args) throws URISyntaxException, SQLException, MissingEnvException, UnirestException, AuthenticationException {
    ArgumentChecker argumentChecker = new ArgumentChecker(args);

    ConnectionDetails connectionDetails = ConnectionDetails.getConnectionDetails(argumentChecker);
    SQLConnection connection = PostgresConnectionFactory.initiateDBConnect(connectionDetails.getDbUrl());

    ExternalServiceHandler externalServiceHandler = new ExternalServiceHandler(connection, ExternalServiceType.TVDB);
    TVDBJWTProvider tvdbjwtProvider = new TVDBJWTProviderImpl(externalServiceHandler);
    JSONReaderImpl jsonReader = new JSONReaderImpl();

    NewSeriesChecker newSeriesChecker = new NewSeriesChecker(connection, tvdbjwtProvider, jsonReader);
    newSeriesChecker.runUpdate();
  }

  @Override
  public String getRunnerName() {
    return "New Series Checker";
  }

  @Override
  public @Nullable UpdateMode getUpdateMode() {
    return null;
  }

  @Override
  public void runUpdate() throws SQLException, UnirestException, AuthenticationException {
    String sql = "SELECT * " +
        "FROM series " +
        "WHERE first_processed = ? " +
        "ORDER BY id ";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, false);

    while (resultSet.next()) {
      Series series = new Series();
      series.initializeFromDBObject(resultSet);

      // DO STUFF
      logger.info("New Series found: '" + series.seriesTitle.getValue() + "'");

      updateTVDB(series);
      updateMetacritic(series);
      updateCloudinary(series);

      series.firstProcessed.changeValue(true);
      series.commit(connection);
    }
  }

  private void updateTVDB(Series series) throws SQLException, UnirestException, AuthenticationException {
    TVDBSeriesUpdater updater = new TVDBSeriesUpdater(connection, series, tvdbjwtProvider, jsonReader);
    try {
      updater.updateSeries();
    } catch (ShowFailedException e) {
      logger.warn(e.getMessage());
    }
  }

  private void updateMetacritic(Series series) throws SQLException {
    MetacriticTVUpdater metacriticTVUpdater = new MetacriticTVUpdater(series, connection);

    try {
      metacriticTVUpdater.parseMetacritic();
    } catch (MetacriticException e) {
      logger.warn(e.getMessage());
    }
  }

  private void updateCloudinary(Series series) throws SQLException {
    CloudinaryUpdater cloudinaryUpdater = new CloudinaryUpdater(Singleton.getCloudinary(), series, connection);

    try {
      cloudinaryUpdater.updateSeries();
    } catch (ShowFailedException e) {

      logger.warn("Series failed: " + e.getLocalizedMessage());
    }
  }
}
