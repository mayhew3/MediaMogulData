package com.mayhew3.mediamogul.tv;

import com.mashape.unirest.http.exceptions.UnirestException;
import com.mayhew3.mediamogul.ArgumentChecker;
import com.mayhew3.mediamogul.ExternalServiceHandler;
import com.mayhew3.mediamogul.ExternalServiceType;
import com.mayhew3.mediamogul.db.ConnectionDetails;
import com.mayhew3.mediamogul.exception.MissingEnvException;
import com.mayhew3.mediamogul.model.tv.Episode;
import com.mayhew3.mediamogul.model.tv.Series;
import com.mayhew3.mediamogul.model.tv.TVDBConnectionLog;
import com.mayhew3.mediamogul.model.tv.TVDBUpdateError;
import com.mayhew3.mediamogul.scheduler.UpdateRunner;
import com.mayhew3.mediamogul.socket.MySocketFactory;
import com.mayhew3.mediamogul.socket.SocketWrapper;
import com.mayhew3.mediamogul.tv.exception.ShowFailedException;
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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONObject;

import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

public class TVDBUpdateRunner implements UpdateRunner {

  private final Map<UpdateMode, Runnable> methodMap;

  private enum SeriesUpdateResult {UPDATE_SUCCESS, UPDATE_FAILED}

  private Integer seriesUpdates = 0;
  private Integer episodesAdded = 0;
  private Integer episodesUpdated = 0;

  private SQLConnection connection;

  private TVDBJWTProvider tvdbjwtProvider;
  private JSONReader jsonReader;
  private final SocketWrapper socket;

  private TVDBConnectionLog tvdbConnectionLog;
  private UpdateMode updateMode;

  private static Logger logger = LogManager.getLogger(TVDBUpdateRunner.class);

  @SuppressWarnings("FieldCanBeLocal")
  private final Integer ERROR_FOLLOW_UP_THRESHOLD_IN_DAYS = 7;
  private final Integer ERROR_THRESHOLD = 5;

  public TVDBUpdateRunner(SQLConnection connection, TVDBJWTProvider tvdbjwtProvider, JSONReader jsonReader, SocketWrapper socket, @NotNull UpdateMode updateMode) {
    this.socket = socket;

    methodMap = new HashMap<>();
    methodMap.put(UpdateMode.FULL, this::runFullUpdate);
    methodMap.put(UpdateMode.SMART, this::runSmartUpdateSingleQuery);
    methodMap.put(UpdateMode.RECENT, this::runUpdateOnRecentUpdateList);
    methodMap.put(UpdateMode.FEW_ERRORS, this::runUpdateOnRecentlyErrored);
    methodMap.put(UpdateMode.OLD_ERRORS, this::runUpdateOnOldErrors);
    methodMap.put(UpdateMode.SINGLE, this::runUpdateSingle);
    methodMap.put(UpdateMode.AIRTIMES, this::runAirTimesUpdate);
    methodMap.put(UpdateMode.QUICK, this::runQuickUpdate);
    methodMap.put(UpdateMode.SANITY, this::runSanityUpdateOnShowsThatHaventBeenUpdatedInAWhile);
    methodMap.put(UpdateMode.MANUAL, this::runManuallyQueuedUpdates);

    this.connection = connection;
    this.tvdbjwtProvider = tvdbjwtProvider;
    this.jsonReader = jsonReader;

    if (!methodMap.containsKey(updateMode)) {
      throw new IllegalArgumentException("Update type '" + updateMode + "' is not applicable for this updater.");
    }

    this.updateMode = updateMode;
  }

  public static void main(String... args) throws URISyntaxException, SQLException, UnirestException, MissingEnvException {
    ArgumentChecker argumentChecker = new ArgumentChecker(args);
    argumentChecker.addExpectedOption("socketEnv", true, "Socket environment to connect to.");

    String socketEnv = argumentChecker.getRequiredValue("socketEnv");

    UpdateMode updateMode = UpdateMode.getUpdateModeOrDefault(argumentChecker, UpdateMode.SMART);
    ConnectionDetails connectionDetails = ConnectionDetails.getConnectionDetails(argumentChecker);

    SQLConnection connection = PostgresConnectionFactory.initiateDBConnect(connectionDetails.getDbUrl());
    ExternalServiceHandler tvdbServiceHandler = new ExternalServiceHandler(connection, ExternalServiceType.TVDB);

    String appRole = argumentChecker.getRequiredValue("appRole");

    SocketWrapper socket = new MySocketFactory().createSocket(socketEnv, appRole);

    TVDBUpdateRunner tvdbUpdateRunner = new TVDBUpdateRunner(
        connection,
        new TVDBJWTProviderImpl(tvdbServiceHandler),
        new JSONReaderImpl(),
        socket,
        updateMode);
    tvdbUpdateRunner.runUpdate();

    socket.disconnect();
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
    tvdbConnectionLog.updateType.changeValue(updateMode.getTypekey());
  }

  @Override
  public String getRunnerName() {
    return "TVDB Updater";
  }

  @Override
  public @Nullable UpdateMode getUpdateMode() {
    return updateMode;
  }

  /**
   * Go to theTVDB and update all matched series in my DB with the ones from theirs.
   */
  private void runFullUpdate() {
    String sql = "select * " +
        "from series " +
        "where tvdb_match_status = ? " +
        "and retired = ? ";

    try {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, TVDBMatchStatus.MATCH_COMPLETED, 0);
      runUpdateOnResultSet(resultSet);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Go to theTVDB and only update series that recently had their match confirmed by a user, but haven't yet updated their
   * episodes or series data.
   */
  private void runQuickUpdate() {
    String sql = "select * " +
        "from series " +
        "where tvdb_match_status = ? " +
        "and consecutive_tvdb_errors < ? " +
        "and retired = ? ";

    try {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, TVDBMatchStatus.MATCH_CONFIRMED, ERROR_THRESHOLD, 0);
      runUpdateOnResultSet(resultSet);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  // todo: run on shows that haven't been updated in the past week. Test first: queries to see how many this is.
  private void runSanityUpdateOnShowsThatHaventBeenUpdatedInAWhile() {
    Set<Series> allSeries = new HashSet<>();
    allSeries.addAll(getEligibleTierOneSeries());
    allSeries.addAll(getEligibleTierTwoSeries());
    allSeries.addAll(getEligibleUnownedShows());

    runUpdateForSeriesSet(allSeries);
  }

  private Set<Series> getEligibleTierOneSeries() {
    Set<Series> serieses = new HashSet<>();

    String sql = "select * " +
        "from series " +
        "where tvdb_match_status = ? " +
        "and last_tvdb_update is not null " +
        "and retired = ? " +
        "and (last_tvdb_update > last_tvdb_error or last_tvdb_error is null) " +
        "and id in (select series_id" +
        "           from person_series " +
        "           where tier = ?" +
        "           and retired = ?) " +
        "order by last_tvdb_update " +
        "limit 2 ";

    try {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, TVDBMatchStatus.MATCH_COMPLETED, 0, 1, 0);
      while (resultSet.next()) {
        Series series = new Series();
        series.initializeFromDBObject(resultSet);
        serieses.add(series);
      }
      return serieses;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private Set<Series> getEligibleTierTwoSeries() {
    Set<Series> serieses = new HashSet<>();

    String sql = "select * " +
        "from series " +
        "where tvdb_match_status = ? " +
        "and last_tvdb_update is not null " +
        "and retired = ? " +
        "and (last_tvdb_update > last_tvdb_error or last_tvdb_error is null) " +
        "and id in (select series_id" +
        "           from person_series " +
        "           where tier = ?" +
        "           and retired = ?) " +
        "and id not in (select series_id " +
        "               from person_series " +
        "               where tier = ?" +
        "               and retired = ?) " +
        "order by last_tvdb_update " +
        "limit 1 ";

    try {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, TVDBMatchStatus.MATCH_COMPLETED, 0, 2, 0, 1, 0);
      while (resultSet.next()) {
        Series series = new Series();
        series.initializeFromDBObject(resultSet);
        serieses.add(series);
      }
      return serieses;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private Set<Series> getEligibleUnownedShows() {
    Set<Series> serieses = new HashSet<>();

    String sql = "select * " +
        "from series " +
        "where tvdb_match_status = ? " +
        "and last_tvdb_update is not null " +
        "and retired = ? " +
        "and (last_tvdb_update > last_tvdb_error or last_tvdb_error is null) " +
        "and id not in (select series_id " +
        "           from person_series " +
        "           where retired = ?) " +
        "order by last_tvdb_update " +
        "limit 1 ";

    try {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, TVDBMatchStatus.MATCH_COMPLETED, 0, 0);
      while (resultSet.next()) {
        Series series = new Series();
        series.initializeFromDBObject(resultSet);
        serieses.add(series);
      }
      return serieses;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void runManuallyQueuedUpdates() {
    String sql = "select * " +
        "from series " +
        "where tvdb_manual_queue = ? " +
        "and retired = ? ";

    try {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, true, 0);
      runUpdateOnResultSet(resultSet);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }


  private void runUpdateSingle() {
    String singleSeriesTitle = "Lost"; // update for testing on a single series

    String sql = "select * " +
        "from series " +
        "where title = ? " +
        "and retired = ? ";

    try {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, singleSeriesTitle, 0);
      runUpdateOnResultSet(resultSet);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void runAirTimesUpdate() {
    String singleSeriesTitle = "Detroit Steel"; // update for testing on a single series

    String sql = "select * " +
        "from series " +
        "where title = ? " +
        "and retired = ? ";

    try {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, singleSeriesTitle, 0);

      while (resultSet.next()) {
        Series series = new Series();
        series.initializeFromDBObject(resultSet);

        debug("Updating series '" + series.seriesTitle.getValue() + "'");

        List<Episode> episodes = series.getEpisodes(connection);

        for (Episode episode : episodes) {
          episode.updateAirTime(series.airTime.getValue());
          episode.commit(connection);
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }


  private void runUpdateOnRecentUpdateList() {

    try {
      Timestamp mostRecentSuccessfulUpdate = getMostRecentSuccessfulUpdate();

      validateLastUpdate(mostRecentSuccessfulUpdate);

      logger.info("Finding all episodes updated since: " + mostRecentSuccessfulUpdate);

      JSONObject updatedSeries = tvdbjwtProvider.getUpdatedSeries(mostRecentSuccessfulUpdate);

      if (updatedSeries.isNull("data")) {
        logger.info("Empty list of TVDB updated.");
        return;
      }

      @NotNull JSONArray seriesArray = jsonReader.getArrayWithKey(updatedSeries, "data");

      logger.info("Total series found: " + seriesArray.length());

      for (int i = 0; i < seriesArray.length(); i++) {
        JSONObject seriesRow = seriesArray.getJSONObject(i);
        @NotNull Integer seriesId = jsonReader.getIntegerWithKey(seriesRow, "id");

        String sql = "select * " +
            "from series " +
            "where tvdb_match_status = ? " +
            "and tvdb_series_ext_id = ? " +
            "and retired = ? ";

        @NotNull ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, TVDBMatchStatus.MATCH_COMPLETED, seriesId, 0);
        if (resultSet.next()) {
          Series series = new Series();

          try {
            SeriesUpdateResult updateResult = processSingleSeries(resultSet, series);
            if (SeriesUpdateResult.UPDATE_SUCCESS.equals(updateResult)) {
              tvdbConnectionLog.updatedShows.increment(1);
            } else {
              tvdbConnectionLog.failedShows.increment(1);
            }
          } catch (Exception e) {
            logger.error("Show failed on initialization from DB.");
          }
        } else {
          logger.error("Recently updated series not found: ID " + seriesId);
        }
      }
    } catch (SQLException | UnirestException | AuthenticationException e) {
      throw new RuntimeException(e);
    }
  }

  private void validateLastUpdate(Timestamp mostRecentSuccessfulUpdate) {
    DateTime mostRecent = new DateTime(mostRecentSuccessfulUpdate);
    DateTime sixDaysAgo = new DateTime().minusDays(6);
    if (mostRecent.isBefore(sixDaysAgo)) {
      throw new IllegalStateException("No updates in 6 days! Need to run a full update to catch up!");
    }
  }

  private void runUpdateOnRecentlyErrored() {
    String sql = "select * " +
        "from series " +
        "where last_tvdb_error is not null " +
        "and consecutive_tvdb_errors < ? " +
        "and tvdb_match_status = ? " +
        "and retired = ? ";

    try {
      @NotNull ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, ERROR_THRESHOLD, TVDBMatchStatus.MATCH_COMPLETED, 0);
      runUpdateOnResultSet(resultSet);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void runUpdateOnOldErrors() {
    DateTime now = new DateTime(new Date());
    DateTime aWeekAgo = now.minusDays(ERROR_FOLLOW_UP_THRESHOLD_IN_DAYS);
    Timestamp timestamp = new Timestamp(aWeekAgo.toDate().getTime());

    String sql = "select * " +
        "from series " +
        "where last_tvdb_error is not null " +
        "and last_tvdb_error < ? " +
        "and consecutive_tvdb_errors >= ? " +
        "and tvdb_match_status = ? " +
        "and retired = ? ";

    try {
      @NotNull ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, timestamp, ERROR_THRESHOLD, TVDBMatchStatus.MATCH_COMPLETED, 0);
      runUpdateOnResultSet(resultSet);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void runUpdateOnResultSet(ResultSet resultSet) throws SQLException {
    logger.info("Starting update.");

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
        logger.error("Show failed on initialization from DB.");
      }

      seriesUpdates++;
    }

    logger.info("Update complete for result set: " + i + " processed.");
  }

  private void runUpdateForSeriesSet(Set<Series> serieses) {
    logger.info("Starting update.");

    List<Series> sortedSerieses = serieses.stream()
        .sorted()
        .collect(Collectors.toList());

    int i = 0;

    for (Series series : sortedSerieses) {
      i++;

      try {
        @NotNull SeriesUpdateResult result = runUpdateOnSingleSeries(series, false);
        if (result.equals(SeriesUpdateResult.UPDATE_SUCCESS)) {
          tvdbConnectionLog.updatedShows.increment(1);
        } else {
          tvdbConnectionLog.failedShows.increment(1);
        }
      } catch (Exception e) {
        logger.error("Show failed on initialization from DB.");
      }

      seriesUpdates++;
    }

    logger.info("Update complete for result set: " + i + " processed.");
  }

  private void runSmartUpdateSingleQuery() {

    debug("Starting update.");

    String sql = "select * " +
        "from series " +
        "where retired = ? ";

    try {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, 0);

      int i = 0;

      while (resultSet.next()) {
        i++;
        Series series = new Series();
        series.initializeFromDBObject(resultSet);

        if (shouldUpdateSeries(series)) {

          try {
            @NotNull SeriesUpdateResult result = processSingleSeries(resultSet, series);
            if (result.equals(SeriesUpdateResult.UPDATE_SUCCESS)) {
              tvdbConnectionLog.updatedShows.increment(1);
            } else {
              tvdbConnectionLog.failedShows.increment(1);
            }
          } catch (Exception e) {
            logger.error("Show failed on initialization from DB.");
          }

          seriesUpdates++;
        }
      }

      debug("Update complete for series: " + (i-1) + " processed.");
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private Boolean shouldUpdateSeries(Series series) {
    return isRecentlyErrored(series) ||
        isOldErrored(series) ||
        matchReadyToComplete(series) ||
        hasUnmatchedEpisodes(series);
  }

  private Boolean isRecentlyErrored(Series series) {
    return hasError(series) &&
        withinConsecutiveErrorThreshold(series) &&
        hasMatchStatus(series, TVDBMatchStatus.MATCH_COMPLETED);
  }

  private Boolean isOldErrored(Series series) {
    DateTime now = new DateTime(new Date());
    DateTime aWeekAgo = now.minusDays(ERROR_FOLLOW_UP_THRESHOLD_IN_DAYS);
    Timestamp timestamp = new Timestamp(aWeekAgo.toDate().getTime());

    return hasError(series) &&
        withinErrorDateThreshold(series, timestamp) &&
        hasMatchStatus(series, TVDBMatchStatus.MATCH_COMPLETED);
  }

  private Boolean matchReadyToComplete(Series series) {
    return hasMatchStatus(series, TVDBMatchStatus.MATCH_CONFIRMED) &&
        withinConsecutiveErrorThreshold(series);
  }



  private Boolean withinConsecutiveErrorThreshold(Series series) {
    return series.consecutiveTVDBErrors.getValue() < ERROR_THRESHOLD;
  }

  private Boolean hasMatchStatus(Series series, String matchStatus) {
    return matchStatus.equals(series.tvdbMatchStatus.getValue());
  }

  private Boolean hasError(Series series) {
    return series.lastTVDBError.getValue() != null;
  }

  private Boolean withinErrorDateThreshold(Series series, Timestamp timestamp) {
    return series.lastTVDBError.getValue().before(timestamp);
  }

  private Boolean hasUnmatchedEpisodes(Series series) {
    DateTime now = new DateTime(new Date());
    DateTime aDayAgo = now.minusDays(1);
    Timestamp timestamp = new Timestamp(aDayAgo.toDate().getTime());

    return series.lastTVDBUpdate.getValue() != null &&
        series.lastTVDBUpdate.getValue().before(timestamp) &&
        !series.isSuggestion.getValue() &&
        series.unmatchedEpisodes.getValue() > 0;
  }

  @NotNull
  private SeriesUpdateResult processSingleSeries(ResultSet resultSet, Series series) throws SQLException {
    boolean addingSeries = false;
    if (!series.isInitialized()) {
      series.initializeFromDBObject(resultSet);
      addingSeries = true;
    }

    return runUpdateOnSingleSeries(series, addingSeries);
  }

  private @NotNull SeriesUpdateResult runUpdateOnSingleSeries(Series series, boolean addingSeries) throws SQLException {
    try {
      updateTVDB(series);
      if (addingSeries) {
        maybeUpdateSeriesRequest(series);
      }
      resetTVDBErrors(series);
      return SeriesUpdateResult.UPDATE_SUCCESS;
    } catch (Exception e) {
      e.printStackTrace();
      debug("Show failed TVDB: " + series.seriesTitle.getValue());
      updateTVDBErrors(series);
      addUpdateError(e, series);
      return SeriesUpdateResult.UPDATE_FAILED;
    }
  }

  private void maybeUpdateSeriesRequest(Series series) throws SQLException {
    String sql = "UPDATE series_request " +
        "SET completed = ? " +
        "WHERE tvdb_series_ext_id = ? " +
        "AND completed IS NULL " +
        "AND retired = ? ";

    connection.prepareAndExecuteStatementUpdate(sql,
        new Timestamp(new Date().getTime()),
        series.tvdbSeriesExtId.getValue(),
        0);
  }

  private void addUpdateError(Exception e, Series series) throws SQLException {
    TVDBUpdateError tvdbUpdateError = new TVDBUpdateError();
    tvdbUpdateError.initializeForInsert();

    tvdbUpdateError.context.changeValue("TVDBUpdateRunner");
    tvdbUpdateError.exceptionClass.changeValue(e.getClass().toString());
    tvdbUpdateError.exceptionMsg.changeValue(e.getMessage());
    tvdbUpdateError.seriesId.changeValue(series.id.getValue());

    tvdbUpdateError.commit(connection);
  }



  private Timestamp getMostRecentSuccessfulUpdate() throws SQLException {
    String sql = "select max(start_time) as max_start_time " +
        "from tvdb_connection_log " +
        "where update_type in (?, ?, ?) " +
        "and finish_time is not null";
    @NotNull ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql,
        UpdateMode.FULL.getTypekey(),
        UpdateMode.SMART.getTypekey(),
        UpdateMode.RECENT.getTypekey()
    );
    if (resultSet.next()) {
      Calendar calendar = Calendar.getInstance();
      calendar.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
      Timestamp maxStartTime = resultSet.getTimestamp("max_start_time");
      if (maxStartTime == null) {
        throw new IllegalStateException("Max start time should never be null.");
      } else {
        return maxStartTime;
      }
    } else {
      throw new IllegalStateException("Max start time should never be an empty set.");
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

  private void updateTVDB(Series series) throws SQLException, ShowFailedException, UnirestException, AuthenticationException {
    TVDBSeriesUpdater updater = new TVDBSeriesUpdater(connection, series, tvdbjwtProvider, jsonReader, socket);
    updater.updateSeries();

    episodesAdded += updater.getEpisodesAdded();
    episodesUpdated += updater.getEpisodesUpdated();
  }

  private Integer getSeriesUpdates() {
    return seriesUpdates;
  }

  public Integer getEpisodesAdded() {
    return episodesAdded;
  }

  public Integer getEpisodesUpdated() {
    return episodesUpdated;
  }



  private void debug(Object message) {
    logger.debug(message);
  }

}

