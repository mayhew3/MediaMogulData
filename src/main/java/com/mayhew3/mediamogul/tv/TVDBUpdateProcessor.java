package com.mayhew3.mediamogul.tv;

import com.google.common.collect.Lists;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.mayhew3.mediamogul.scheduler.UpdateRunner;
import com.mayhew3.postgresobject.db.SQLConnection;
import com.mayhew3.mediamogul.model.tv.Series;
import com.mayhew3.mediamogul.model.tv.TVDBConnectionLog;
import com.mayhew3.mediamogul.model.tv.TVDBUpdateError;
import com.mayhew3.mediamogul.model.tv.TVDBWorkItem;
import com.mayhew3.mediamogul.tv.exception.ShowFailedException;
import com.mayhew3.mediamogul.tv.helper.UpdateMode;
import com.mayhew3.mediamogul.tv.provider.TVDBJWTProvider;
import com.mayhew3.mediamogul.xml.BadlyFormattedXMLException;
import com.mayhew3.mediamogul.xml.JSONReader;
import org.apache.http.auth.AuthenticationException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;

public class TVDBUpdateProcessor implements UpdateRunner {


  private enum SeriesUpdateResult {UPDATE_SUCCESS, UPDATE_FAILED}

  private SQLConnection connection;
  private TVDBJWTProvider tvdbjwtProvider;
  private JSONReader jsonReader;

  private TVDBConnectionLog tvdbConnectionLog;

  // todo: add a failure_time field instead. just don't retry a work item, let the failure updater handle it.
  @SuppressWarnings("FieldCanBeLocal")
  private final Integer ERROR_THRESHOLD = 3;

  public TVDBUpdateProcessor(SQLConnection connection, TVDBJWTProvider tvdbjwtProvider, JSONReader jsonReader) {
    this.connection = connection;
    this.tvdbjwtProvider = tvdbjwtProvider;
    this.jsonReader = jsonReader;
  }

  @Override
  public String getRunnerName() {
    return "TVDB Update Processor";
  }

  @Override
  public @Nullable UpdateMode getUpdateMode() {
    return null;
  }

  @SuppressWarnings("InfiniteLoopStatement")
  public void runUpdate() throws SQLException, UnirestException {
    List<TVDBWorkItem> workItems = getUnprocessedWorkItems();
    if (workItems.isEmpty()) {
      debug("No series in queue.");
    } else {
      debug("Updating " + workItems.size() + " shows...");
      runPeriodicUpdate(workItems);
    }
  }

  private void runPeriodicUpdate(List<TVDBWorkItem> workItems) throws SQLException, UnirestException {

    List<WorkItemGroup> workItemGroups = groupWorkItems(workItems);

    initializeConnectionLog(UpdateMode.SERVICE);

    try {
      for (WorkItemGroup workItemGroup : workItemGroups) {
        try {
          SeriesUpdateResult updateResult = processWorkItemGroup(workItemGroup);
          if (SeriesUpdateResult.UPDATE_SUCCESS.equals(updateResult)) {
            tvdbConnectionLog.updatedShows.increment(1);
          } else {
            tvdbConnectionLog.failedShows.increment(1);
          }
        } catch (Exception e) {
          debug("Show failed on initialization from DB.");
        }

      }

      tvdbConnectionLog.finishTime.changeValue(new Date());

    } catch (Exception e) {
      e.printStackTrace();
      addUpdateError(e);
    } finally {
      tvdbConnectionLog.commit(connection);
      tvdbConnectionLog = null;
    }
  }


  private void addUpdateError(Exception e) throws SQLException {
    TVDBUpdateError tvdbUpdateError = new TVDBUpdateError();
    tvdbUpdateError.initializeForInsert();

    tvdbUpdateError.context.changeValue("TVDBUpdateProcessor");
    tvdbUpdateError.exceptionClass.changeValue(e.getClass().toString());
    tvdbUpdateError.exceptionMsg.changeValue(e.getMessage());

    tvdbUpdateError.commit(connection);
  }



  private void initializeConnectionLog(@NotNull UpdateMode updateMode) {
    tvdbConnectionLog = new TVDBConnectionLog();
    tvdbConnectionLog.initializeForInsert();

    tvdbConnectionLog.startTime.changeValue(new Date());
    tvdbConnectionLog.updatedShows.changeValue(0);
    tvdbConnectionLog.failedShows.changeValue(0);
    tvdbConnectionLog.updateType.changeValue(updateMode.getTypekey());
  }

  private @NotNull SeriesUpdateResult processWorkItemGroup(WorkItemGroup workItemGroup) throws SQLException {
    Series series = null;

    try {
      series = getSeries(workItemGroup);

      updateTVDB(series);
      resetTVDBErrors(series);
      updateWorkItems(workItemGroup);

      return SeriesUpdateResult.UPDATE_SUCCESS;
    } catch (Exception e) {
      e.printStackTrace();
      if (series == null) {
        debug(workItemGroup.workItems.size() + " work items failed for series id: " + workItemGroup.seriesId);
      } else {
        debug("Series TVDB failed: " + series.seriesTitle.getValue());
        updateTVDBErrors(series);
      }
      return SeriesUpdateResult.UPDATE_FAILED;
    }
  }

  private void updateWorkItems(WorkItemGroup workItemGroup) throws SQLException {
    for (TVDBWorkItem workItem : workItemGroup.workItems) {
      workItem.processedTime.changeValue(now());
      workItem.commit(connection);
    }
  }

  @NotNull
  private Timestamp now() {
    return new Timestamp(new Date().getTime());
  }

  private Series getSeries(WorkItemGroup workItemGroup) throws SQLException {
    Integer seriesId = workItemGroup.seriesId;

    String sql = "select * " +
        "from series " +
        "where id = ? " +
        "and retired = ? ";
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, seriesId, 0);
    if (resultSet.next()) {
      Series series = new Series();
      series.initializeFromDBObject(resultSet);
      return series;
    }
    throw new IllegalStateException("Work item with invalid series id: " + seriesId);
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
    TVDBSeriesUpdater updater = new TVDBSeriesUpdater(connection, series, tvdbjwtProvider, jsonReader);
    updater.updateSeries();
  }


  private List<TVDBWorkItem> getUnprocessedWorkItems() throws SQLException {
    List<TVDBWorkItem> workItems = new ArrayList<>();

    String sql = "select twi.* " +
        "from tvdb_work_item twi " +
        "inner join series s " +
        " on twi.series_id = s.id " +
        "where processed_time is null " +
        "and s.consecutive_tvdb_errors < ? " +
        "order by twi.id asc ";
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, ERROR_THRESHOLD);
    while(resultSet.next()) {
      TVDBWorkItem workItem = new TVDBWorkItem();
      workItem.initializeFromDBObject(resultSet);

      workItems.add(workItem);
    }
    return workItems;
  }

  private List<WorkItemGroup> groupWorkItems(List<TVDBWorkItem> workItems) {
    List<WorkItemGroup> groups = new ArrayList<>();
    for (TVDBWorkItem workItem : workItems) {
      Optional<WorkItemGroup> matchingGroup = groups
          .stream()
          .filter(group -> group.seriesId.equals(workItem.seriesId.getValue()))
          .findFirst();

      if (matchingGroup.isPresent()) {
        matchingGroup.get().addWorkItem(workItem);
      } else {
        WorkItemGroup workItemGroup = new WorkItemGroup(workItem.seriesId.getValue(), Lists.newArrayList(workItem));
        groups.add(workItemGroup);
      }
    }
    return groups;
  }

  private class WorkItemGroup {
    private Integer seriesId;
    private List<TVDBWorkItem> workItems;

    private WorkItemGroup(Integer seriesId, List<TVDBWorkItem> workItems) {
      this.seriesId = seriesId;
      this.workItems = workItems;
    }

    void addWorkItem(TVDBWorkItem workItem) {
      workItems.add(workItem);
    }
  }

  protected static void debug(Object message) {
    System.out.println(new Date() + ": " + message);
  }

}
