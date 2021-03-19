package com.mayhew3.mediamogul.tv.utility;

import com.mashape.unirest.http.exceptions.UnirestException;
import com.mayhew3.mediamogul.ExternalServiceHandler;
import com.mayhew3.mediamogul.ExternalServiceType;
import com.mayhew3.mediamogul.db.DatabaseEnvironments;
import com.mayhew3.mediamogul.model.tv.Series;
import com.mayhew3.mediamogul.tv.TVDBMatchStatus;
import com.mayhew3.mediamogul.tv.provider.TVDBJWTProvider;
import com.mayhew3.mediamogul.tv.provider.TVDBJWTProviderImpl;
import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.db.DatabaseEnvironment;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
import com.mayhew3.postgresobject.exception.MissingEnvException;
import org.apache.http.auth.AuthenticationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("FieldCanBeLocal")
public class TVDBJSONFetcher {

  private String singleSeriesTitle = "Inside Amy Schumer"; // update for testing on a single series

  private SQLConnection connection;

  private static Logger logger = LogManager.getLogger(TVDBJSONFetcher.class);

  private TVDBJSONFetcher(SQLConnection connection) {
    this.connection = connection;
  }

  public static void main(String... args) throws URISyntaxException, SQLException, IOException, UnirestException, AuthenticationException, MissingEnvException {
    ArgumentChecker argumentChecker = new ArgumentChecker(args);

    DatabaseEnvironment environment = DatabaseEnvironments.getEnvironmentForDBArgument(argumentChecker);
    SQLConnection connection = PostgresConnectionFactory.createConnection(environment);
    TVDBJSONFetcher tvdbJsonFetcher = new TVDBJSONFetcher(connection);

    tvdbJsonFetcher.downloadJSONForSeries();
  }

  private void downloadJSONForSeries() throws SQLException, IOException, UnirestException, AuthenticationException, MissingEnvException {

    String sql = "select *\n" +
        "from series\n" +
        "where tvdb_match_status = ? " +
        "and title = ? " +
        "and retired = ? ";
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, TVDBMatchStatus.MATCH_COMPLETED, singleSeriesTitle, 0);

    logger.info("Starting update.");


    if (resultSet.next()) {
      Series series = new Series();

      series.initializeFromDBObject(resultSet);
      Integer tvdbSeriesId = series.tvdbSeriesExtId.getValue();

      ExternalServiceHandler tvdbServiceHandler = new ExternalServiceHandler(connection, ExternalServiceType.TVDB);
      TVDBJWTProviderImpl tvdbjwtProvider = new TVDBJWTProviderImpl(tvdbServiceHandler);

      tvdbjwtProvider.writeSearchToFile(singleSeriesTitle);
      tvdbjwtProvider.writeSeriesToFile(tvdbSeriesId);

      // NOTE: After every run, need to truncate the _episodes.json file to only include the episodes below.
      // todo: figure out a way to update it automatically? Maybe use the query to get each episode and manually
      // todo: concatenate each data json into a json array? Or can I manipulate the JSON object directly,
      // todo: and remove the episodes I don't want?
//      tvdbjwtProvider.writeEpisodeSummariesToFile(tvdbSeriesId);

      List<TVDBJWTProvider.EpisodeDetail> episodeNumbers = new ArrayList<>();
      episodeNumbers.add(new TVDBJWTProvider.EpisodeDetail(4, 1));
      episodeNumbers.add(new TVDBJWTProvider.EpisodeDetail(4, 2));
      episodeNumbers.add(new TVDBJWTProvider.EpisodeDetail(4, 3));
      tvdbjwtProvider.writeEpisodeDetailsToFiles(tvdbSeriesId, episodeNumbers);

      tvdbjwtProvider.writePostersToFile(tvdbSeriesId);

    } else {
      throw new IllegalStateException("Series not found: " + singleSeriesTitle);
    }
  }


  private void debug(Object message) {
    logger.debug(message);
  }

}

