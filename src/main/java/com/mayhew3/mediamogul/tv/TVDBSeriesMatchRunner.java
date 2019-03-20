package com.mayhew3.mediamogul.tv;

import com.mashape.unirest.http.exceptions.UnirestException;
import com.mayhew3.mediamogul.ExternalServiceHandler;
import com.mayhew3.mediamogul.ExternalServiceType;
import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
import com.mayhew3.mediamogul.model.tv.Series;
import com.mayhew3.mediamogul.model.tv.TVDBConnectionLog;
import com.mayhew3.mediamogul.model.tv.TVDBEpisode;
import com.mayhew3.mediamogul.model.tv.TiVoEpisode;
import com.mayhew3.mediamogul.scheduler.UpdateRunner;
import com.mayhew3.mediamogul.tv.exception.ShowFailedException;
import com.mayhew3.mediamogul.tv.helper.UpdateMode;
import com.mayhew3.mediamogul.tv.provider.TVDBJWTProvider;
import com.mayhew3.mediamogul.tv.provider.TVDBJWTProviderImpl;
import com.mayhew3.mediamogul.xml.BadlyFormattedXMLException;
import com.mayhew3.mediamogul.xml.JSONReader;
import com.mayhew3.mediamogul.xml.JSONReaderImpl;
import org.apache.http.auth.AuthenticationException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.joda.time.DateTime;

import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class TVDBSeriesMatchRunner implements UpdateRunner {

  private enum SeriesUpdateResult {UPDATE_SUCCESS, UPDATE_FAILED}

  private SQLConnection connection;

  private TVDBJWTProvider tvdbjwtProvider;
  private JSONReader jsonReader;

  private TVDBConnectionLog tvdbConnectionLog;
  private UpdateMode updateMode;

  private final Map<UpdateMode, Runnable> methodMap;

  private final Integer ERROR_THRESHOLD = 3;

  @SuppressWarnings("FieldCanBeLocal")
  private final Integer ERROR_FOLLOW_UP_THRESHOLD_IN_DAYS = 7;

  public TVDBSeriesMatchRunner(SQLConnection connection, TVDBJWTProvider tvdbjwtProvider, JSONReader jsonReader, UpdateMode updateMode) {
    methodMap = new HashMap<>();
    methodMap.put(UpdateMode.FIRST_PASS, this::runFirstPassUpdate);
    methodMap.put(UpdateMode.SMART, this::runSmartUpdate);
    methodMap.put(UpdateMode.FEW_ERRORS, this::runUpdateOnRecentlyErrored);
    methodMap.put(UpdateMode.OLD_ERRORS, this::runUpdateOnOldErrors);
    methodMap.put(UpdateMode.SINGLE, this::runUpdateSingle);
    methodMap.put(UpdateMode.EPISODE_MATCH, this::tryToMatchTiVoEpisodes);

    this.connection = connection;
    this.tvdbjwtProvider = tvdbjwtProvider;
    this.jsonReader = jsonReader;

    if (!methodMap.keySet().contains(updateMode)) {
      throw new IllegalArgumentException("Update type '" + updateMode + "' is not applicable for this updater.");
    }

    this.updateMode = updateMode;
  }

  public static void main(String... args) throws URISyntaxException, SQLException, UnirestException {
    ArgumentChecker argumentChecker = new ArgumentChecker(args);
    UpdateMode updateMode = UpdateMode.getUpdateModeOrDefault(argumentChecker, UpdateMode.FIRST_PASS);

    SQLConnection connection = PostgresConnectionFactory.createConnection(argumentChecker);

    ExternalServiceHandler tvdbServiceHandler = new ExternalServiceHandler(connection, ExternalServiceType.TVDB);
    TVDBSeriesMatchRunner tvdbUpdateRunner = new TVDBSeriesMatchRunner(connection, new TVDBJWTProviderImpl(tvdbServiceHandler), new JSONReaderImpl(), updateMode);
    tvdbUpdateRunner.runUpdate();
  }

  public void runUpdate() throws SQLException {

    initializeConnectionLog(updateMode);

    try {
      methodMap.get(updateMode).run();
      tvdbConnectionLog.finishTime.changeValue(new Date());
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      tvdbConnectionLog.commit(connection);
      tvdbConnectionLog = null;
    }

  }

  private void initializeConnectionLog(@NotNull UpdateMode updateMode) {
    tvdbConnectionLog = new TVDBConnectionLog();
    tvdbConnectionLog.initializeForInsert();

    tvdbConnectionLog.startTime.changeValue(new Date());
    tvdbConnectionLog.updatedShows.changeValue(0);
    tvdbConnectionLog.failedShows.changeValue(0);
    tvdbConnectionLog.updateType.changeValue(updateMode.getTypekey() + " (match only)");
  }

  @Override
  public String getRunnerName() {
    return "TVDB Series Matcher";
  }

  @Override
  public @Nullable UpdateMode getUpdateMode() {
    return updateMode;
  }

  /**
   * Go to theTVDB and update all series in my DB with the ones from theirs.
   */
  private void runFirstPassUpdate() {
    String sql = "select *\n" +
        "from series\n" +
        "where tvdb_match_status = ? " +
        "and last_tvdb_error is null " +
        "and retired = ? ";

    try {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, TVDBMatchStatus.MATCH_FIRST_PASS, 0);

      runUpdateOnResultSet(resultSet);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }



  private void runUpdateSingle() {
    String singleSeriesTitle = "Sneaky Pete"; // update for testing on a single series

    String sql = "select *\n" +
        "from series\n" +
        "where title = ? " +
        "and retired = ? ";
    try {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, singleSeriesTitle, 0);

      runUpdateOnResultSet(resultSet);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void runSmartUpdate() {

    debug("");
    debug("-- STARTING UPDATE FOR NEEDS FIRST PASS SHOWS WITH NO ERRORS -- ");
    debug("");

    runFirstPassUpdate();

    debug("");
    debug("-- STARTING UPDATE FOR RECENTLY FAILED SHOWS WITH FEW ERRORS -- ");
    debug("");

    runUpdateOnRecentlyErrored();

    debug("");
    debug("-- STARTING UPDATE FOR OLD FAILED SHOWS WITH MANY ERRORS -- ");
    debug("");

    runUpdateOnOldErrors();

    debug("");
    debug("-- STARTING UPDATE FOR ALL UNMATCHED EPISODES -- ");
    debug("");

    tryToMatchTiVoEpisodes();
  }

  private void runUpdateOnRecentlyErrored() {
    String sql = "select *\n" +
        "from series\n" +
        "where tvdb_match_status = ? " +
        "and last_tvdb_error is not null " +
        "and consecutive_tvdb_errors < ? " +
        "and retired = ? ";

    try {
      @NotNull ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, TVDBMatchStatus.MATCH_FIRST_PASS, ERROR_THRESHOLD, 0);
      runUpdateOnResultSet(resultSet);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void runUpdateOnOldErrors() {
    DateTime now = new DateTime(new Date());
    DateTime aWeekAgo = now.minusDays(ERROR_FOLLOW_UP_THRESHOLD_IN_DAYS);
    Timestamp timestamp = new Timestamp(aWeekAgo.toDate().getTime());

    String sql = "select *\n" +
        "from series\n" +
        "where last_tvdb_error is not null\n" +
        "and last_tvdb_error < ?\n" +
        "and consecutive_tvdb_errors >= ? " +
        "and retired = ? ";

    try {
      @NotNull ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, timestamp, ERROR_THRESHOLD, 0);
      runUpdateOnResultSet(resultSet);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void tryToMatchTiVoEpisodes() {
    String sql =
        "SELECT * " +
            "FROM tivo_episode " +
            "WHERE (tvdb_match_status IS NULL OR tvdb_match_status = ?) " +
            "AND retired = ? ";

    try {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, TVDBMatchStatus.MATCH_FIRST_PASS, 0);

      while (resultSet.next()) {
        TiVoEpisode tiVoEpisode = new TiVoEpisode();
        tiVoEpisode.initializeFromDBObject(resultSet);

        try {
          Series series = getSeries(tiVoEpisode);

          debug("Looking for match for TiVo Episode: " + tiVoEpisode);

          TVDBEpisodeMatcher tvdbEpisodeMatcher = new TVDBEpisodeMatcher(connection, tiVoEpisode, series.id.getValue());
          Optional<TVDBEpisode> tvdbEpisodeOptional = tvdbEpisodeMatcher.matchAndLinkEpisode();

          if (tvdbEpisodeOptional.isPresent()) {
            TVDBEpisode tvdbEpisode = tvdbEpisodeOptional.get();
            debug("- Match Found! Linked to episode: " + tvdbEpisode);
          } else {
            debug("- No Match Found.");
          }

        } catch (ShowFailedException e) {
          e.printStackTrace();
          debug("Error finding series associated with TiVoEpisode: " + tiVoEpisode);
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private Series addNewSeries(TiVoEpisode tiVoEpisode) throws SQLException {
    Series series = new Series();

    series.initializeForInsert();

    String tivoId = tiVoEpisode.tivoSeriesV2ExtId.getValue();
    Integer tivoVersion = 2;
    if (tivoId == null) {
      tivoId = tiVoEpisode.tivoSeriesExtId.getValue();
      tivoVersion = 1;
    }

    debug("Adding series '" + tiVoEpisode.seriesTitle.getValue() + "'  with TiVoID '" + tivoId + "'");

    series.initializeDenorms();

    series.tivoSeriesV2ExtId.changeValue(tivoId);
    series.seriesTitle.changeValue(tiVoEpisode.seriesTitle.getValue());
    series.tivoName.changeValue(tiVoEpisode.seriesTitle.getValue());
    if (tiVoEpisode.suggestion.getValue() != null) {
      series.isSuggestion.changeValue(tiVoEpisode.suggestion.getValue());
    }
    series.matchedWrong.changeValue(false);
    series.tvdbNew.changeValue(true);
    series.metacriticNew.changeValue(true);
    series.tivoVersion.changeValue(tivoVersion);
    series.addedBy.changeValue("TiVo");
    series.tvdbMatchStatus.changeValue(TVDBMatchStatus.MATCH_FIRST_PASS);

    series.commit(connection);

    series.addViewingLocation(connection, "TiVo");

    return series;
  }


  private Series getSeries(TiVoEpisode tiVoEpisode) throws SQLException, ShowFailedException {
    String tivoSeriesExtId = tiVoEpisode.tivoSeriesV2ExtId.getValue();
    if (tivoSeriesExtId == null) {
      return getSeriesFromV1(tiVoEpisode);
    }

    String sql =
        "SELECT * " +
            "FROM series " +
            "WHERE tivo_series_v2_ext_id = ? " +
            "AND retired = ? ";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, tivoSeriesExtId, 0);

    if (resultSet.next()) {
      Series series = new Series();
      series.initializeFromDBObject(resultSet);
      return series;
    } else {
      debug("Unable to find existing series with v2 ID: " + tivoSeriesExtId + ". Adding new series.");
      return addNewSeries(tiVoEpisode);
    }
  }

  private Series getSeriesFromV1(TiVoEpisode tiVoEpisode) throws ShowFailedException, SQLException {
    String tivoSeriesExtId = tiVoEpisode.tivoSeriesExtId.getValue();
    if (tivoSeriesExtId == null) {
      throw new ShowFailedException("TiVo Episode with no V2 or V1 series id: " + tiVoEpisode);
    }

    String sql =
        "SELECT * " +
            "FROM series " +
            "WHERE tivo_series_ext_id = ? " +
            "AND retired = ? ";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, tivoSeriesExtId, 0);

    if (resultSet.next()) {
      Series series = new Series();
      series.initializeFromDBObject(resultSet);
      return series;
    } else {
      debug("Unable to find series with V1 ID: " + tivoSeriesExtId + ". Adding new series.");
      return addNewSeries(tiVoEpisode);
    }
  }

  private void runUpdateOnResultSet(ResultSet resultSet) throws SQLException {
    debug("Starting update.");

    int i = 0;

    while (resultSet.next()) {
      i++;
      Series series = new Series();

      try {
        @NotNull SeriesUpdateResult result = processSingleSeries(resultSet, series);
        if (result.equals(SeriesUpdateResult.UPDATE_SUCCESS)) {
          tvdbConnectionLog.updatedShows.increment(1);
        } else {
          tvdbConnectionLog.failedShows.increment(1);
        }
      } catch (Exception e) {
        debug("Show failed on initialization from DB.");
      }
    }

    debug("Update complete for result set: " + i + " processed.");
  }

  @NotNull
  private SeriesUpdateResult processSingleSeries(ResultSet resultSet, Series series) throws SQLException {
    series.initializeFromDBObject(resultSet);

    try {
      updateTVDB(series);
      resetTVDBErrors(series);
      return SeriesUpdateResult.UPDATE_SUCCESS;
    } catch (Exception e) {
      e.printStackTrace();
      debug("Show failed TVDB: " + series.seriesTitle.getValue());
      updateTVDBErrors(series);
      return SeriesUpdateResult.UPDATE_FAILED;
    }
  }

  private void updateTVDBErrors(Series series) throws SQLException {
    series.lastTVDBError.changeValue(new Date());
    series.consecutiveTVDBErrors.increment(1);
    series.commit(connection);
  }

  private void resetTVDBErrors(Series series) throws SQLException {
    series.lastTVDBError.changeValue(null);
    series.consecutiveTVDBErrors.changeValue(0);
    series.commit(connection);
  }

  private void updateTVDB(Series series) throws SQLException, BadlyFormattedXMLException, ShowFailedException, UnirestException, AuthenticationException {
    TVDBSeriesMatchUpdater updater = new TVDBSeriesMatchUpdater(connection, series, tvdbjwtProvider, jsonReader);
    updater.updateSeries();
  }


  protected void debug(Object message) {
    System.out.println(message);
  }

}

