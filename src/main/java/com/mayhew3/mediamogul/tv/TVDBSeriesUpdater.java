package com.mayhew3.mediamogul.tv;

import com.google.common.base.Preconditions;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.mayhew3.mediamogul.model.tv.*;
import com.mayhew3.mediamogul.tv.exception.ShowFailedException;
import com.mayhew3.mediamogul.tv.provider.TVDBJWTProvider;
import com.mayhew3.mediamogul.xml.JSONReader;
import com.mayhew3.mediamogul.xml.JSONReaderImpl;
import com.mayhew3.postgresobject.dataobject.FieldValue;
import com.mayhew3.postgresobject.db.SQLConnection;
import org.apache.http.auth.AuthenticationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class TVDBSeriesUpdater {

  private Series series;
  private List<Episode> episodes;
  private List<TVDBEpisode> tvdbEpisodes;

  private SQLConnection connection;
  private TVDBJWTProvider tvdbDataProvider;
  private JSONReader jsonReader;

  private Set<Integer> foundEpisodeIds;
  private Set<Integer> erroredEpisodeIds;

  private Integer episodesAdded = 0;
  private Integer episodesUpdated = 0;
  private Integer episodesFailed = 0;

  private static Logger logger = LogManager.getLogger(TVDBSeriesUpdater.class);

  TVDBSeriesUpdater(SQLConnection connection,
                    @NotNull Series series,
                    TVDBJWTProvider tvdbWebProvider,
                    JSONReader jsonReader) {
    this.series = series;
    this.connection = connection;
    this.tvdbDataProvider = tvdbWebProvider;
    this.jsonReader = jsonReader;
    this.foundEpisodeIds = new HashSet<>();
    this.erroredEpisodeIds = new HashSet<>();
  }


  void updateSeries() throws SQLException, ShowFailedException, UnirestException, AuthenticationException {
    String seriesTitle = series.seriesTitle.getValue();

    episodes = series.getEpisodes(connection);
    tvdbEpisodes = series.getTVDBEpisodes(connection);

    debug(seriesTitle + ": ID found, getting show data.");

    updateShowData();
  }

  private void updateShowData() throws SQLException, UnirestException, AuthenticationException, ShowFailedException {
    debug("UpdateShowData...");
    if (series.tvdbMatchId.getValue() != null && TVDBMatchStatus.MATCH_CONFIRMED.equals(series.tvdbMatchStatus.getValue())) {
      series.tvdbSeriesExtId.changeValue(series.tvdbMatchId.getValue());
    }

    Integer tvdbSeriesExtId = series.tvdbSeriesExtId.getValue();

    if (tvdbSeriesExtId == null) {
      throw new ShowFailedException("Updater trying to process series with null TVDB ID: " + series);
    }

    String seriesTitle = series.seriesTitle.getValue();

    JSONObject seriesRoot = tvdbDataProvider.getSeriesData(tvdbSeriesExtId);

    debug(seriesTitle + ": Data found, updating.");

    JSONObject seriesJson = seriesRoot.getJSONObject("data");

    TVDBSeries tvdbSeries = getTVDBSeries(tvdbSeriesExtId);

    updateTVDBSeries(tvdbSeriesExtId, seriesJson, tvdbSeries);

    // If we are finalizing the series match for the first time, add it to the collection of the person who made the add request.
    if (series.addedByUser.getValue() != null &&
        !TVDBMatchStatus.MATCH_COMPLETED.equalsIgnoreCase(series.tvdbMatchStatus.getValue()) &&
        !isSeriesAlreadyInPersonCollection(series)) {
      addPersonSeriesForRequestOwner(series);
    }

    Integer tvdbSeriesId = tvdbSeries.id.getValue();
    series.tvdbSeriesId.changeValue(tvdbSeriesId);
    series.lastTVDBUpdate.changeValue(new Date());
    series.tvdbManualQueue.changeValue(false);

    JSONArray genres = jsonReader.getArrayWithKey(seriesJson, "genre");
    updateGenres(series, genres);

    series.commit(connection);

    debug("Finished series update.");

    updateAllEpisodes(tvdbSeriesExtId);
    updateOnlyAbsoluteNumbers();

    series.tvdbNew.changeValue(false);
    series.commit(connection);

    // Change API version if no episodes failed.
    if (episodesFailed == 0) {
      tvdbSeries.apiVersion.changeValue(2);
      tvdbSeries.commit(connection);
    }

    series.tvdbMatchStatus.changeValue(TVDBMatchStatus.MATCH_COMPLETED);

    series.commit(connection);

    debug(seriesTitle + ": Update complete! Added: " + episodesAdded + "; Updated: " + episodesUpdated);

  }

  private Boolean isSeriesAlreadyInPersonCollection(Series series) throws SQLException {
    Preconditions.checkArgument(series.addedByUser.getValue() != null, "Cannot check for existing PersonSeries for series with null person_id.");

    Integer personId = series.addedByUser.getValue();
    Integer seriesId = series.id.getValue();

    String sql = "SELECT 1 " +
        "FROM person_series " +
        "WHERE person_id = ? " +
        "AND series_id = ? " +
        "AND retired = ? ";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, personId, seriesId, 0);
    return resultSet.next();
  }

  private void addPersonSeriesForRequestOwner(Series series) throws SQLException {
    Preconditions.checkArgument(series.addedByUser.getValue() != null, "Cannot add PersonSeries for series with null person_id.");

    Integer personId = series.addedByUser.getValue();
    Integer seriesId = series.id.getValue();

    PersonSeries personSeries = new PersonSeries();
    personSeries.initializeForInsert();
    personSeries.personId.changeValue(personId);
    personSeries.seriesId.changeValue(seriesId);
    personSeries.tier.changeValue(1);
    personSeries.unwatchedEpisodes.changeValue(0);
    personSeries.unwatchedStreaming.changeValue(0);

    personSeries.commit(connection);
  }

  private void updateOnlyAbsoluteNumbers() throws SQLException {
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(
        "SELECT e.* " +
            "FROM episode e " +
            "WHERE e.series_id = ? " +
            "AND e.retired = ? " +
            "AND e.air_time IS NOT NULL " +
            "ORDER BY e.season, e.episode_number ", series.id.getValue(), 0);

    int i = 1;
    while (resultSet.next()) {
      Episode episode = new Episode();
      episode.initializeFromDBObject(resultSet);

      episode.absoluteNumber.changeValue(i);
      episode.commit(connection);

      i++;
    }
  }


  private <T> void updateLinkedFieldsIfNotOverridden(FieldValue<T> slaveField, FieldValue<T> masterField, @Nullable T newValue) {
    if (slaveField.getValue() == null ||
        slaveField.getValue().equals(masterField.getValue())) {
      slaveField.changeValue(newValue);
    }
    masterField.changeValue(newValue);
  }

  private void updateAllEpisodes(Integer tvdbID) {
    Integer pageNumber = 1;
    Integer lastPage;

    do {

      try {
        JSONObject episodeData = tvdbDataProvider.getEpisodeSummaries(tvdbID, pageNumber);

        JSONObject links = episodeData.getJSONObject("links");
        lastPage = jsonReader.getIntegerWithKey(links, "last");
        debug("Page " + pageNumber + " of " + lastPage + "...");

        JSONArray episodeArray = episodeData.getJSONArray("data");

        for (int i = 0; i < episodeArray.length(); i++) {
          JSONObject episode = episodeArray.getJSONObject(i);
          updateEpisode(episode);
        }
      } catch (Exception e) {
        logger.warn("Error fetching episode data for series with TVDB ID: " + tvdbID);
        lastPage = 0;
      }

      pageNumber++;

    } while (pageNumber <= lastPage);
    debug("end updateAllEpisodes.");
  }

  @NotNull
  private TVDBSeries getTVDBSeries(Integer tvdbID) throws SQLException, ShowFailedException {
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(
        "SELECT * " +
            "FROM tvdb_series " +
            "WHERE tvdb_series_ext_id = ? " +
            "and retired = ? ",
        tvdbID, 0
    );

    TVDBSeries tvdbSeries = new TVDBSeries();

    if (resultSet.next()) {
      tvdbSeries.initializeFromDBObject(resultSet);
    } else {
      if (TVDBMatchStatus.MATCH_COMPLETED.equals(series.tvdbMatchStatus.getValue())) {
        throw new ShowFailedException("All 'Match Completed' shows should have a corresponding tvdb_series object.");
      } else {
        tvdbSeries.initializeForInsert();
      }
    }

    return tvdbSeries;
  }

  private void updateEpisode(JSONObject episode) throws SQLException {
    Integer episodeRemoteId = episode.getInt("id");
    debug("updateEpisode " + episode.getInt("airedSeason") + "x" + episode.getInt("airedEpisodeNumber") + ": " + episodeRemoteId);

    try {
      TVDBEpisodeUpdater tvdbEpisodeUpdater = new TVDBEpisodeUpdater(
          series,
          connection,
          episodeRemoteId,
          new JSONReaderImpl(),
          episodes,
          tvdbEpisodes,
          episode);
      TVDBEpisodeUpdater.EPISODE_RESULT episodeResult = tvdbEpisodeUpdater.updateSingleEpisode();

      if (episodeResult == TVDBEpisodeUpdater.EPISODE_RESULT.ADDED) {
        episodesAdded++;
      } else if (episodeResult == TVDBEpisodeUpdater.EPISODE_RESULT.UPDATED) {
        episodesUpdated++;
      }

      if (episodeResult != TVDBEpisodeUpdater.EPISODE_RESULT.RETIRED) {
        foundEpisodeIds.add(episodeRemoteId);
      }
    } catch (Exception e) {
      debug("TVDB update of episode failed: ");
      e.printStackTrace();
      episodesFailed++;
      erroredEpisodeIds.add(episodeRemoteId);
      updateEpisodeLastError(episodeRemoteId);
      addUpdateError(episodeRemoteId, e);
      addMigrationError(episodeRemoteId, e);
    }
  }

  private void updateTVDBSeries(Integer tvdbID, JSONObject seriesJson, TVDBSeries tvdbSeries) throws SQLException {
    String tvdbSeriesName = jsonReader.getStringWithKey(seriesJson, "seriesName");

    Integer id = jsonReader.getIntegerWithKey(seriesJson, "id");

    tvdbSeries.tvdbSeriesExtId.changeValue(id);
    tvdbSeries.name.changeValue(tvdbSeriesName);
    tvdbSeries.airsDayOfWeek.changeValue(jsonReader.getNullableStringWithKey(seriesJson, "airsDayOfWeek"));

    updateLinkedFieldsIfNotOverridden(series.airTime, tvdbSeries.airsTime, jsonReader.getNullableStringWithKey(seriesJson, "airsTime"));

    tvdbSeries.firstAired.changeValueFromString(jsonReader.getNullableStringWithKey(seriesJson, "firstAired"));
    tvdbSeries.network.changeValue(jsonReader.getNullableStringWithKey(seriesJson, "network"));
    tvdbSeries.overview.changeValue(jsonReader.getNullableStringWithKey(seriesJson, "overview"));
    tvdbSeries.rating.changeValue(jsonReader.getNullableDoubleWithKey(seriesJson, "siteRating"));
    tvdbSeries.ratingCount.changeValue(jsonReader.getNullableIntegerWithKey(seriesJson, "siteRatingCount"));
    tvdbSeries.runtime.changeValueFromString(jsonReader.getNullableStringWithKey(seriesJson, "runtime"));
    tvdbSeries.status.changeValue(jsonReader.getNullableStringWithKey(seriesJson, "status"));

    tvdbSeries.banner.changeValueFromString(jsonReader.getNullableStringWithKey(seriesJson, "banner"));

    // todo: change to integer in data model
    tvdbSeries.lastUpdated.changeValueFromString(((Integer)seriesJson.getInt("lastUpdated")).toString());
    tvdbSeries.imdbId.changeValueFromString(jsonReader.getNullableStringWithKey(seriesJson, "imdbId"));
    tvdbSeries.zap2it_id.changeValueFromString(jsonReader.getNullableStringWithKey(seriesJson, "zap2itId"));

    // todo: 'added' field
    // todo: 'networkid' field

    // todo: add api_version column to tvdb_series and tvdb_episode, and change it when this finishes processing.
    // todo: create api_change_log table and add a row for each change to series or episode
    // todo: create tvdb_error_log table and log any json format issues where non-nullable are null, or values are wrong type.

    Boolean isForInsert = tvdbSeries.isForInsert();

    // if we are inserting, need to commit before adding posters, which will reference tvdb_series.id
    if (isForInsert) {
      tvdbSeries.commit(connection);
    }

    Optional<TVDBPoster> optionalLastAdded = updatePosters(tvdbID, tvdbSeries);
    if (optionalLastAdded.isPresent()) {
      TVDBPoster lastAdded = optionalLastAdded.get();
      tvdbSeries.lastPoster.changeValue(lastAdded.posterPath.getValue());
      series.poster.changeValue(lastAdded.posterPath.getValue());
      series.cloud_poster.changeValue(optionalLastAdded.get().cloud_poster.getValue());
    }

    // only add change log if an existing series is changing, not for a new one.
    if (!isForInsert && tvdbSeries.hasChanged()) {
      addChangeLogs(tvdbSeries);
    }

    tvdbSeries.commit(connection);
  }

  private void updateGenres(Series series, JSONArray genreArray) throws SQLException {
    List<String> added = new ArrayList<>();
    for (int i = 0; i < genreArray.length(); i++) {
      String genre = genreArray.getString(i);
      Optional<SeriesGenre> seriesGenre = series.addGenre(connection, genre);
      if (seriesGenre.isPresent()) {
        added.add(genre);
      }
    }
    logger.info(added + " genres added for series '" + series.seriesTitle.getValue() + "'");
  }

  private Optional<TVDBPoster> updatePosters(Integer tvdbID, TVDBSeries tvdbSeries)  {

    try {
      JSONObject imageData = tvdbDataProvider.getPosterData(tvdbID);
      @NotNull JSONArray images = jsonReader.getArrayWithKey(imageData, "data");

      if (images.length() == 0) {
        return Optional.empty();
      }

      Optional<TVDBPoster> mostRecentPoster = Optional.empty();

      for (int i = 0; i < images.length(); i++) {
        JSONObject image = images.getJSONObject(i);
        @NotNull String filename = jsonReader.getStringWithKey(image, "fileName");
        mostRecentPoster = tvdbSeries.addPosterIfDoesntExist(filename, null, connection);
      }

      return mostRecentPoster;
    } catch (Exception e) {
      logger.warn("Error fetching posters for series: " + tvdbSeries.name.getValue());
      return Optional.empty();
    }
  }

  private void updateEpisodeLastError(Integer tvdbEpisodeExtId) {
    String sql = "SELECT e.* " +
        "FROM episode e " +
        "INNER JOIN tvdb_episode te " +
        " ON te.id = e.tvdb_episode_id " +
        "WHERE te.tvdb_episode_ext_id = ? " +
        "AND te.retired = ?";
    try {
      @NotNull ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, tvdbEpisodeExtId, 0);
      if (resultSet.next()) {
        Episode episode = new Episode();
        episode.initializeFromDBObject(resultSet);
        episode.lastTVDBError.changeValue(new Date());
        episode.commit(connection);
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  private void addMigrationError(Integer tvdbEpisodeExtId, Exception e) throws SQLException {
    TVDBMigrationError migrationError = new TVDBMigrationError();
    migrationError.initializeForInsert();

    migrationError.seriesId.changeValue(series.id.getValue());
    migrationError.tvdbEpisodeExtId.changeValue(tvdbEpisodeExtId);
    migrationError.exceptionType.changeValue(e.getClass().toString());
    migrationError.exceptionMsg.changeValue(e.getMessage());

    migrationError.commit(connection);
  }

  private void addUpdateError(Integer tvdbEpisodeExtId, Exception e) throws SQLException {
    TVDBUpdateError tvdbUpdateError = new TVDBUpdateError();
    tvdbUpdateError.initializeForInsert();

    tvdbUpdateError.context.changeValue("TVDBSeriesUpdater");
    tvdbUpdateError.exceptionClass.changeValue(e.getClass().toString());
    tvdbUpdateError.exceptionMsg.changeValue(e.getMessage());
    tvdbUpdateError.seriesId.changeValue(series.id.getValue());
    tvdbUpdateError.tvdbEpisodeExtId.changeValue(tvdbEpisodeExtId);

    tvdbUpdateError.commit(connection);
  }



  private void addChangeLogs(TVDBSeries tvdbSeries) throws SQLException {
    for (FieldValue fieldValue : tvdbSeries.getChangedFields()) {
      TVDBMigrationLog tvdbMigrationLog = new TVDBMigrationLog();
      tvdbMigrationLog.initializeForInsert();

      tvdbMigrationLog.tvdbSeriesId.changeValue(tvdbSeries.id.getValue());

      tvdbMigrationLog.tvdbFieldName.changeValue(fieldValue.getFieldName());
      tvdbMigrationLog.oldValue.changeValue(fieldValue.getOriginalValue() == null ?
          null :
          fieldValue.getOriginalValue().toString());
      tvdbMigrationLog.newValue.changeValue(fieldValue.getChangedValue() == null ?
          null :
          fieldValue.getChangedValue().toString());

      tvdbMigrationLog.commit(connection);
    }
  }


  private void debug(Object message) {
    logger.debug(message);
  }


  Integer getEpisodesAdded() {
    return episodesAdded;
  }

  Integer getEpisodesUpdated() {
    return episodesUpdated;
  }

}
