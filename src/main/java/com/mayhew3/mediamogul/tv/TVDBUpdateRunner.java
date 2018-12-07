package com.mayhew3.mediamogul.tv;

import com.mashape.unirest.http.exceptions.UnirestException;
import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
import com.mayhew3.mediamogul.model.tv.Series;
import com.mayhew3.mediamogul.model.tv.TVDBConnectionLog;
import com.mayhew3.mediamogul.model.tv.TVDBUpdateError;
import com.mayhew3.mediamogul.scheduler.UpdateRunner;
import com.mayhew3.mediamogul.tv.exception.ShowFailedException;
import com.mayhew3.mediamogul.tv.helper.UpdateMode;
import com.mayhew3.mediamogul.tv.provider.TVDBJWTProvider;
import com.mayhew3.mediamogul.tv.provider.TVDBJWTProviderImpl;
import com.mayhew3.mediamogul.xml.JSONReader;
import com.mayhew3.mediamogul.xml.JSONReaderImpl;
import org.apache.http.auth.AuthenticationException;
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

public class TVDBUpdateRunner implements UpdateRunner {

  private final Map<UpdateMode, Runnable> methodMap;

  private enum SeriesUpdateResult {UPDATE_SUCCESS, UPDATE_FAILED}

  private Integer seriesUpdates = 0;
  private Integer episodesAdded = 0;
  private Integer episodesUpdated = 0;

  private SQLConnection connection;

  private TVDBJWTProvider tvdbjwtProvider;
  private JSONReader jsonReader;

  private TVDBConnectionLog tvdbConnectionLog;
  private UpdateMode updateMode;

  @SuppressWarnings("FieldCanBeLocal")
  private final Integer ERROR_FOLLOW_UP_THRESHOLD_IN_DAYS = 7;
  private final Integer ERROR_THRESHOLD = 5;

  public TVDBUpdateRunner(SQLConnection connection, TVDBJWTProvider tvdbjwtProvider, JSONReader jsonReader, @NotNull UpdateMode updateMode) {

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

    if (!methodMap.keySet().contains(updateMode)) {
      throw new IllegalArgumentException("Update type '" + updateMode + "' is not applicable for this updater.");
    }

    this.updateMode = updateMode;
  }

  public static void main(String... args) throws URISyntaxException, SQLException, UnirestException {
    ArgumentChecker argumentChecker = new ArgumentChecker(args);

    UpdateMode updateMode = UpdateMode.getUpdateModeOrDefault(argumentChecker, UpdateMode.SMART);

    SQLConnection connection = PostgresConnectionFactory.createConnection(argumentChecker);

    TVDBUpdateRunner tvdbUpdateRunner = new TVDBUpdateRunner(connection, new TVDBJWTProviderImpl(), new JSONReaderImpl(), updateMode);
    tvdbUpdateRunner.runUpdate();

    if (tvdbUpdateRunner.getSeriesUpdates() > 0) {
      // update denorms after changes.
      new SeriesDenormUpdater(connection).runUpdate();
    }
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
    String sql = "select *\n" +
        "from series\n" +
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
    DateTime today = new DateTime();
    Timestamp sevenDaysAgo = new Timestamp(today.minusDays(7).getMillis());
    Timestamp threeDaysAgo = new Timestamp(today.minusDays(3).getMillis());
    Timestamp threeMonthsAgo = new Timestamp(today.minusMonths(3).getMillis());

    String sql = "select *\n" +
        "from series\n" +
        "where tvdb_match_status = ? " +
        "and last_tvdb_update is not null\n" +
        "and suggestion = ?\n" +
        "and retired = ?\n" +
        "and ((last_tvdb_update < ? and tier = ?) " +
          "or (last_tvdb_update < ? and most_recent > ?))";

    try {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, TVDBMatchStatus.MATCH_COMPLETED, false, 0, threeDaysAgo, 1, sevenDaysAgo, threeMonthsAgo);
      runUpdateOnResultSet(resultSet);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void runManuallyQueuedUpdates() {
    String sql = "select *\n" +
        "from series\n" +
        "where tvdb_manual_queue = ? " +
        "and retired = ?;";

    try {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, true, 0);
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

  private void runAirTimesUpdate() {
    String singleSeriesTitle = "Detroit Steel"; // update for testing on a single series

    String sql = "select *\n" +
        "from series\n" +
        "where title = ? " +
        "and retired = ? ";

    try {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, singleSeriesTitle, 0);

      while (resultSet.next()) {
        Series series = new Series();
        series.initializeFromDBObject(resultSet);

        debug("Updating series '" + series.seriesTitle.getValue() + "'");

        TVDBEpisodeUpdater tvdbEpisodeUpdater = new TVDBEpisodeUpdater(series, connection, tvdbjwtProvider, 1, jsonReader, false);
        tvdbEpisodeUpdater.updateOnlyAirTimes();
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }


  private void runUpdateOnRecentUpdateList() {

    try {
      Timestamp mostRecentSuccessfulUpdate = getMostRecentSuccessfulUpdate();

      validateLastUpdate(mostRecentSuccessfulUpdate);

      debug("Finding all episodes updated since: " + mostRecentSuccessfulUpdate);

      JSONObject updatedSeries = tvdbjwtProvider.getUpdatedSeries(mostRecentSuccessfulUpdate);

      if (updatedSeries.isNull("data")) {
        debug("Empty list of TVDB updated.");
        return;
      }

      @NotNull JSONArray seriesArray = jsonReader.getArrayWithKey(updatedSeries, "data");

      debug("Total series found: " + seriesArray.length());

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
            debug("Show failed on initialization from DB.");
          }
        } else {
          debug("Recently updated series not found: ID " + seriesId);
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
    String sql = "select *\n" +
        "from series\n" +
        "where last_tvdb_error is not null\n" +
        "and consecutive_tvdb_errors < ?\n" +
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

    String sql = "select *\n" +
        "from series\n" +
        "where last_tvdb_error is not null\n" +
        "and last_tvdb_error < ?\n" +
        "and consecutive_tvdb_errors >= ?\n" +
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

      seriesUpdates++;
    }

    debug("Update complete for result set: " + i + " processed.");
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
            debug("Show failed on initialization from DB.");
          }

          seriesUpdates++;
        }
      }

      debug("Update complete for series: " + i + " processed.");
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
    String sql = "select max(start_time) as max_start_time\n" +
        "from tvdb_connection_log\n" +
        "where update_type in (?, ?, ?)\n" +
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
    TVDBSeriesUpdater updater = new TVDBSeriesUpdater(connection, series, tvdbjwtProvider, jsonReader);
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



  protected void debug(Object message) {
    System.out.println(message);
  }

}

