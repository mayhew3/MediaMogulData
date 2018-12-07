package com.mayhew3.mediamogul.tv;

import com.google.common.collect.Lists;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
import com.mayhew3.mediamogul.model.tv.Series;
import com.mayhew3.mediamogul.model.tv.TVDBUpdateError;
import com.mayhew3.mediamogul.model.tv.TVDBWorkItem;
import com.mayhew3.mediamogul.tv.helper.UpdateMode;
import com.mayhew3.mediamogul.tv.provider.TVDBJWTProvider;
import com.mayhew3.mediamogul.tv.provider.TVDBJWTProviderImpl;
import com.mayhew3.mediamogul.xml.JSONReader;
import com.mayhew3.mediamogul.xml.JSONReaderImpl;
import org.apache.http.auth.AuthenticationException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.joda.time.DateTime;
import org.joda.time.Seconds;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import static java.lang.Thread.sleep;

public class TVDBUpdateFinderRunner {

  private SQLConnection connection;
  private TVDBJWTProvider tvdbjwtProvider;
  private JSONReader jsonReader;

  private Timestamp lastUpdated;

  private String identifier;

  private static PrintStream originalStream = System.out;

  private static Boolean logToFile = false;
  private static PrintStream logOutput = null;

  // TVDB doesn't seem to work right with a timestamp less than 120 seconds ago, always returns nothing.
  @SuppressWarnings("FieldCanBeLocal")
  private Integer SECONDS = 120;

  private TVDBUpdateFinderRunner(SQLConnection connection, TVDBJWTProvider tvdbjwtProvider, JSONReader jsonReader, String identifier) {
    this.connection = connection;
    this.tvdbjwtProvider = tvdbjwtProvider;
    this.jsonReader = jsonReader;
    this.identifier = identifier;
  }

  public static void main(String... args) throws UnirestException, URISyntaxException, SQLException, InterruptedException, FileNotFoundException, AuthenticationException {
    List<String> argList = Lists.newArrayList(args);
    logToFile = argList.contains("LogToFile");
    boolean lastWeek = argList.contains("LastWeek");
    boolean healthCheck = argList.contains("HealthCheck");

    ArgumentChecker argumentChecker = new ArgumentChecker(args);

    if (logToFile) {
      openLogStream(argumentChecker.getDBIdentifier());
    }

    debug("");
    debug("SESSION START! Date: " + new Date());
    debug("");

    SQLConnection connection = PostgresConnectionFactory.createConnection(argumentChecker);
    TVDBUpdateFinderRunner tvdbUpdateRunner = new TVDBUpdateFinderRunner(connection, new TVDBJWTProviderImpl(), new JSONReaderImpl(), argumentChecker.getDBIdentifier());

    if (lastWeek) {
      tvdbUpdateRunner.fillInGapsFromPastWeek();
    } else if (healthCheck) {
      tvdbUpdateRunner.testUpdaterWorksSameWithPeriod(90);
    } else {
      tvdbUpdateRunner.runUpdater();
    }
  }

  private static void openLogStream(String identifier) throws FileNotFoundException {
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
    String dateFormatted = simpleDateFormat.format(new Date());

    String mediaMogulLogs = System.getenv("MediaMogulLogs");

    File file = new File(mediaMogulLogs + "\\TVDBUpdateFinder_" + dateFormatted + "_" + identifier + ".log");
    FileOutputStream fos = new FileOutputStream(file, true);
    logOutput = new PrintStream(fos);

    System.setErr(logOutput);
    System.setOut(logOutput);
  }

  private static void closeLogStream() {
    System.setErr(originalStream);
    System.setOut(originalStream);

    logOutput.close();
    logOutput = null;
  }

  @SuppressWarnings("InfiniteLoopStatement")
  private void runUpdater() throws InterruptedException, FileNotFoundException, SQLException {

    while (true) {
      if (logToFile && logOutput == null) {
        openLogStream(identifier);
      }

      debug(new Date());
      debug("Starting periodic update...");

      try {
        runPeriodicUpdate();
        debug("Finished run. Waiting " + SECONDS + " seconds...");
      } catch (AuthenticationException | UnirestException | SQLException | JSONException e) {
        debug("Exception thrown with TVDB! Trying again in " + SECONDS + " seconds...");
        e.printStackTrace();
        addUpdateError(e);
      }

      if (logToFile && logOutput != null) {
        closeLogStream();
      }

      sleep(1000 * SECONDS);
    }
  }

  private void addUpdateError(Exception e) throws SQLException {
    TVDBUpdateError tvdbUpdateError = new TVDBUpdateError();
    tvdbUpdateError.initializeForInsert();

    tvdbUpdateError.context.changeValue("TVDBUpdateFinder");
    tvdbUpdateError.exceptionClass.changeValue(e.getClass().toString());
    tvdbUpdateError.exceptionMsg.changeValue(e.getMessage());

    tvdbUpdateError.commit(connection);
  }



  @SuppressWarnings("SameParameterValue")
  private void testUpdaterWorksSameWithPeriod(Integer seconds) throws SQLException, UnirestException, InterruptedException, AuthenticationException {
    DateTime now = DateTime.now();
    DateTime tenMinutesFromNow = now.plusMinutes(10);

    Timestamp veryFirstStartTime = getLastUpdateTime();

    List<TVDBUpdate> updatesFromPeriodic = new ArrayList<>();

    while (tenMinutesFromNow.isAfterNow()) {
      debug("Starting periodic update...");

      updatesFromPeriodic.addAll(runPeriodicUpdate());

      debug("Finished run. Waiting " + seconds + " seconds...");
      sleep(1000 * seconds);
    }

    List<TVDBUpdate> updatesFromFull = runPeriodicUpdate(veryFirstStartTime);

    // going to assume there are some updates missing from the back end of each update. This test is going to focus
    // on no updates missed from between runs, and expect any missed from the most recent run will be made up for
    // in a successive run. Therefore, we use the final periodic update as a maximum, and make sure all the previous
    // ones are complete.
    Optional<Timestamp> maxUpdateTime = getMaxUpdateTime(updatesFromPeriodic);

    if (maxUpdateTime.isPresent()) {
      validatePeriodicUpdates(updatesFromPeriodic, updatesFromFull, maxUpdateTime.get());
    } else {
      debug("No periodic updates. Aborting comparison.");
    }

  }

  private void fillInGapsFromPastWeek() throws AuthenticationException, UnirestException, SQLException {
    DateTime sixDaysAgo = new DateTime().minusDays(6);
    runPeriodicUpdate(new Timestamp(sixDaysAgo.toDate().getTime()));
  }

  private void validatePeriodicUpdates(List<TVDBUpdate> updatesFromPeriodic, List<TVDBUpdate> updatesFromFull, Timestamp maxUpdateTime) {
    List<TVDBUpdate> eligibleUpdatesFromFull = updatesFromFull.stream()
        .filter(update -> !update.getTvdbLastUpdated().after(maxUpdateTime))
        .collect(Collectors.toList());

    List<TVDBUpdate> missedInPeriodic = Lists.newArrayList(eligibleUpdatesFromFull);
    missedInPeriodic.removeAll(updatesFromPeriodic);

    if (!missedInPeriodic.isEmpty()) {
      throw new RuntimeException("Episodes not found in periodic run: " + missedInPeriodic);
    }
  }

  private Optional<Timestamp> getMaxUpdateTime(List<TVDBUpdate> updates) {
    return updates.stream()
        .sorted(Comparator.comparing(TVDBUpdate::getTvdbLastUpdated).reversed())
        .map(TVDBUpdate::getTvdbLastUpdated)
        .findFirst();
  }

  private List<TVDBUpdate> runPeriodicUpdate() throws SQLException, UnirestException, AuthenticationException {
    Timestamp lastUpdateTime = getLastUpdateTime();
    Timestamp startTime = createStartTimeWithBuffer(lastUpdateTime);
    return runPeriodicUpdate(startTime);
  }

  @NotNull
  private Timestamp createStartTimeWithBuffer(@NotNull Timestamp lastUpdateTime) {
    DateTime startTimeWithBuffer = new DateTime(lastUpdateTime).minusSeconds(120);
    return new Timestamp(startTimeWithBuffer.toDate().getTime());
  }

  @NotNull
  private List<TVDBUpdate> runPeriodicUpdate(@NotNull Timestamp startTime) throws UnirestException, SQLException, AuthenticationException {
    Timestamp now = now();

    Seconds secondsDiff = Seconds.secondsBetween(new DateTime(startTime), new DateTime(now));
    debug("Finding updates between " + startTime + " and " + now + ", diff of " + secondsDiff.getSeconds() + " seconds.");

    JSONObject updatedSeries = tvdbjwtProvider.getUpdatedSeries(startTime);


    if (updatedSeries.isNull("data")) {
      debug("Empty list of TVDB updated.");
      lastUpdated = now;
      return new ArrayList<>();
    }

    List<TVDBUpdate> allUpdates = processTVDBPayload(now, updatedSeries);

    lastUpdated = now;

    return allUpdates;
  }

  private List<TVDBUpdate> processTVDBPayload(Timestamp now, JSONObject updatedSeries) throws SQLException {
    @NotNull JSONArray seriesArray = jsonReader.getArrayWithKey(updatedSeries, "data");

    List<TVDBUpdate> allUpdates = new ArrayList<>();

    debug("Total series found: " + seriesArray.length());

    for (int i = 0; i < seriesArray.length(); i++) {
      JSONObject seriesRow = seriesArray.getJSONObject(i);
      @NotNull Integer tvdbSeriesExtId = jsonReader.getIntegerWithKey(seriesRow, "id");
      @NotNull Integer lastUpdatedEpoch = jsonReader.getIntegerWithKey(seriesRow, "lastUpdated");
      Timestamp tvdbLastUpdated = getFromEpochTime(lastUpdatedEpoch);

      allUpdates.add(new TVDBUpdate(tvdbSeriesExtId, tvdbLastUpdated));

      maybeUpdateSeries(now, tvdbSeriesExtId, tvdbLastUpdated);
    }

    return allUpdates;
  }

  private void maybeUpdateSeries(Timestamp now, Integer tvdbSeriesExtId, Timestamp tvdbLastUpdated) throws SQLException {
    String sql = "select * " +
        "from series " +
        "where tvdb_series_ext_id = ? " +
        "and tvdb_match_status = ? " +
        "and retired = ? ";

    @NotNull ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, tvdbSeriesExtId, TVDBMatchStatus.MATCH_COMPLETED, 0);
    if (resultSet.next()) {
      if (hasWorkItemAlready(tvdbSeriesExtId, tvdbLastUpdated)) {
        debug("Work item already exists for TVDB ID " + tvdbSeriesExtId + " and update time of " + tvdbLastUpdated);
      } else {
        updateSingleSeries(now, tvdbSeriesExtId, tvdbLastUpdated, resultSet);
      }
    } else {
      debug("Recently updated series not found: ID " + tvdbSeriesExtId + ", updated " + tvdbLastUpdated);
    }
  }

  private Boolean hasWorkItemAlready(Integer tvdbSeriesExtId, Timestamp tvdbLastUpdated) throws SQLException {
    String sql = "select * " +
        "from tvdb_work_item " +
        "where tvdb_series_ext_id = ? " +
        "and last_updated <= ?";
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, tvdbSeriesExtId, tvdbLastUpdated);
    return resultSet.next();
  }

  private void updateSingleSeries(Timestamp now, Integer tvdbSeriesExtId, Timestamp tvdbLastUpdated, ResultSet resultSet) throws SQLException {
    Series series = new Series();
    series.initializeFromDBObject(resultSet);

    debug("Found update for series '" + series.seriesTitle.getValue() + "', updated " + tvdbLastUpdated);

    Integer seriesId = series.id.getValue();

    TVDBWorkItem workItem = new TVDBWorkItem();
    workItem.initializeForInsert();

    workItem.tvdbSeriesExtId.changeValue(tvdbSeriesExtId);
    workItem.seriesId.changeValue(seriesId);
    workItem.lastUpdated.changeValue(tvdbLastUpdated);
    workItem.foundTime.changeValue(now);

    workItem.commit(connection);
  }

  @NotNull
  private Timestamp now() {
    return new Timestamp(new Date().getTime());
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

  @Nullable
  private Timestamp getMostRecentPeriodicCheck() throws SQLException {
    String sql = "select max(found_time) as max_update_time " +
        "from tvdb_work_item ";
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql);
    if (resultSet.next()) {
      Calendar calendar = Calendar.getInstance();
      calendar.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
      return resultSet.getTimestamp("max_update_time");
    }
    return null;
  }

  @NotNull
  private Timestamp getLastUpdateTime() throws SQLException {
    return lastUpdated == null ? getLastUpdateTimeFromDB() : lastUpdated;
  }

  @NotNull
  private Timestamp getLastUpdateTimeFromDB() throws SQLException {
    Timestamp mostRecentSuccessfulUpdate = getMostRecentSuccessfulUpdate();
    Timestamp mostRecentPeriodicCheck = getMostRecentPeriodicCheck();

    // todo: after series match is found, check that we haven't updated it more recently.
    if (mostRecentPeriodicCheck != null) {
      return mostRecentPeriodicCheck;
    } else if (mostRecentSuccessfulUpdate != null) {
      return mostRecentSuccessfulUpdate;
    } else {
      throw new IllegalStateException("No update time found in tvdb_connection_log or tvdb_work_item.");
    }
  }

  private Timestamp getFromEpochTime(long epochTime) {
    return new Timestamp(epochTime * 1000L);
  }

  protected static void debug(Object message) {
    System.out.println(message);
  }

  class TVDBUpdate {
    private Integer tvdbSeriesExtId;
    private Timestamp tvdbLastUpdated;

    private TVDBUpdate(Integer tvdbSeriesExtId, Timestamp tvdbLastUpdated) {
      this.tvdbSeriesExtId = tvdbSeriesExtId;
      this.tvdbLastUpdated = tvdbLastUpdated;
    }

    public Integer getTvdbSeriesExtId() {
      return tvdbSeriesExtId;
    }

    Timestamp getTvdbLastUpdated() {
      return tvdbLastUpdated;
    }

    @Override
    public String toString() {
      return "TVDB ID " + tvdbSeriesExtId + ", Updated " + tvdbLastUpdated;
    }

    @Override
    public boolean equals(Object obj) {
      assert obj instanceof TVDBUpdate;
      TVDBUpdate otherUpdate = (TVDBUpdate) obj;
      return otherUpdate.getTvdbSeriesExtId().equals(tvdbSeriesExtId) &&
          otherUpdate.getTvdbLastUpdated().equals(tvdbLastUpdated);
    }
  }
}
