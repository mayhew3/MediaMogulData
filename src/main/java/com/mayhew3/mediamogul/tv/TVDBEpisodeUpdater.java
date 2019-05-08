package com.mayhew3.mediamogul.tv;

import com.mayhew3.mediamogul.model.tv.Episode;
import com.mayhew3.mediamogul.model.tv.Series;
import com.mayhew3.mediamogul.model.tv.TVDBEpisode;
import com.mayhew3.mediamogul.model.tv.TVDBMigrationLog;
import com.mayhew3.mediamogul.xml.JSONReader;
import com.mayhew3.postgresobject.dataobject.FieldValue;
import com.mayhew3.postgresobject.db.SQLConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.json.JSONObject;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

class TVDBEpisodeUpdater {
  enum EPISODE_RESULT {ADDED, UPDATED, RETIRED, NONE}

  private Series series;
  private List<Episode> episodes;
  private List<TVDBEpisode> tvdbEpisodes;

  private JSONObject episodeJson;

  private SQLConnection connection;
  private Integer tvdbRemoteId;
  private JSONReader jsonReader;

  private static Logger logger = LogManager.getLogger(TVDBEpisodeUpdater.class);

  TVDBEpisodeUpdater(Series series,
                     SQLConnection connection,
                     Integer tvdbEpisodeId,
                     JSONReader jsonReader,
                     List<Episode> episodes,
                     List<TVDBEpisode> tvdbEpisodes,
                     JSONObject episodeJSON) {
    this.series = series;
    this.connection = connection;
    this.tvdbRemoteId = tvdbEpisodeId;
    this.jsonReader = jsonReader;
    this.episodes = episodes;
    this.tvdbEpisodes = tvdbEpisodes;
    this.episodeJson = episodeJSON;
  }

  /**
   * @return Whether a new episode was added, or episode was found and updated.
   * @throws SQLException If DB query error
   */
  EPISODE_RESULT updateSingleEpisode() throws SQLException {

    Integer seriesId = series.id.getValue();

    Optional<TVDBEpisode> existingEpisode = findExistingTVDBEpisode();

    @NotNull Integer episodenumber = jsonReader.getIntegerWithKey(episodeJson, "airedEpisodeNumber");
    @Nullable String episodename = jsonReader.getNullableStringWithKey(episodeJson, "episodeName");
    @NotNull Integer seasonnumber = jsonReader.getIntegerWithKey(episodeJson, "airedSeason");
    @Nullable String firstaired = jsonReader.getNullableStringWithKey(episodeJson, "firstAired");

    boolean added = false;
    boolean changed = false;

    TVDBEpisode tvdbEpisode;
    Episode episode;

    if (existingEpisode.isPresent()) {
      tvdbEpisode = existingEpisode.get();
      episode = getEpisodeFromTVDBEpisodeID(tvdbEpisode.id.getValue());
    } else {
      logger.debug("Adding new episode.");

      added = true;

      tvdbEpisode = new TVDBEpisode();
      tvdbEpisode.initializeForInsert();

      episode = new Episode();
      episode.initializeForInsert();
    }

    // todo: Add log entry for when TVDB values change.

    Integer absoluteNumber = jsonReader.getNullableIntegerWithKey(episodeJson, "absoluteNumber");

    tvdbEpisode.tvdbEpisodeExtId.changeValue(tvdbRemoteId);
    episode.seriesId.changeValue(seriesId);

    updateLinkedFieldsIfNotOverridden(episode.episodeNumber, tvdbEpisode.episodeNumber, episodenumber);
    updateSeasonIfNotOverridden(episode, tvdbEpisode, seasonnumber);

    tvdbEpisode.absoluteNumber.changeValue(absoluteNumber);
    tvdbEpisode.name.changeValue(episodename);

    updateLinkedFieldsFromStringIfNotOverridden(episode.airDate, tvdbEpisode.firstAired, firstaired);

    tvdbEpisode.tvdbSeriesId.changeValue(series.tvdbSeriesId.getValue());
    tvdbEpisode.overview.changeValue(jsonReader.getNullableStringWithKey(episodeJson, "overview"));
    tvdbEpisode.productionCode.changeValue(jsonReader.getNullableStringWithKey(episodeJson, "productionCode"));
    tvdbEpisode.rating.changeValue(episodeJson.getDouble("siteRating"));
    tvdbEpisode.ratingCount.changeValue(jsonReader.getNullableIntegerWithKey(episodeJson, "siteRatingCount"));
    tvdbEpisode.director.changeValue(jsonReader.getNullableStringWithKey(episodeJson, "director"));

    // todo: writers array
//    tvdbEpisode.writer.changeValueFromString(episodeJson.getString("writers"));

    tvdbEpisode.lastUpdated.changeValue(jsonReader.getIntegerWithKey(episodeJson, "lastUpdated"));

    tvdbEpisode.tvdbSeasonExtId.changeValue(jsonReader.getNullableIntegerWithKey(episodeJson, "airedSeasonID"));

    tvdbEpisode.filename.changeValue(jsonReader.getNullableStringWithKey(episodeJson, "filename"));

    tvdbEpisode.airsAfterSeason.changeValue(jsonReader.getNullableIntegerWithKey(episodeJson, "airsAfterSeason"));
    tvdbEpisode.airsBeforeSeason.changeValue(jsonReader.getNullableIntegerWithKey(episodeJson, "airsAfterSeason"));
    tvdbEpisode.airsBeforeEpisode.changeValue(jsonReader.getNullableIntegerWithKey(episodeJson, "airsAfterSeason"));

    tvdbEpisode.thumbHeight.changeValueFromString(jsonReader.getNullableStringWithKey(episodeJson, "thumbHeight"));
    tvdbEpisode.thumbWidth.changeValueFromString(jsonReader.getNullableStringWithKey(episodeJson, "thumbWidth"));

    if (tvdbEpisode.hasChanged() && !tvdbEpisode.isForInsert()) {
      changed = true;
      addChangeLogs(tvdbEpisode);
    }

    tvdbEpisode.apiVersion.changeValue(2);

    tvdbEpisode.commit(connection);


    episode.seriesTitle.changeValueFromString(series.seriesTitle.getValue());
    episode.tvdbEpisodeId.changeValue(tvdbEpisode.id.getValue());
    episode.title.changeValue(episodename);
    episode.streaming.changeValue(series.isStreaming(connection));

    episode.updateAirTime(series.airTime.getValue());

    if (episode.hasChanged()) {
      changed = true;
    }

    episode.commit(connection);

    Integer episodeId = tvdbEpisode.id.getValue();

    if (episodeId == null) {
      throw new RuntimeException("_id wasn't populated on Episode with tvdbEpisodeId " + tvdbRemoteId + " after insert.");
    }

    if (added) {
      return EPISODE_RESULT.ADDED;
    } else if (changed) {
      return EPISODE_RESULT.UPDATED;
    } else {
      return EPISODE_RESULT.NONE;
    }
  }

  private void addChangeLogs(TVDBEpisode tvdbEpisode) throws SQLException {
    List<FieldValue> changedFields = tvdbEpisode.getChangedFields();
    changedFields.remove(tvdbEpisode.lastUpdated);

    for (FieldValue fieldValue : changedFields) {
      TVDBMigrationLog tvdbMigrationLog = new TVDBMigrationLog();
      tvdbMigrationLog.initializeForInsert();

      tvdbMigrationLog.tvdbSeriesId.changeValue(tvdbEpisode.tvdbSeriesId.getValue());
      tvdbMigrationLog.tvdbEpisodeId.changeValue(tvdbEpisode.id.getValue());

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

  private <T> void updateLinkedFieldsIfNotOverridden(FieldValue<T> slaveField, FieldValue<T> masterField, @Nullable T newValue) {
    if (slaveField.getValue() == null ||
        slaveField.getValue().equals(masterField.getValue())) {
      slaveField.changeValue(newValue);
    }
    masterField.changeValue(newValue);
  }

  private <T> void updateLinkedFieldsFromStringIfNotOverridden(FieldValue<T> slaveField, FieldValue<T> masterField, @Nullable String newValue) {
    if (slaveField.getValue() == null ||
        slaveField.getValue().equals(masterField.getValue())) {
      slaveField.changeValueFromString(newValue);
    }
    masterField.changeValueFromString(newValue);
  }

  private void updateSeasonIfNotOverridden(Episode episode, TVDBEpisode tvdbEpisode, Integer season) throws SQLException {
    if (episode.getSeason() == null ||
            episode.getSeason().equals(tvdbEpisode.seasonNumber.getValue())) {
      episode.setSeason(season, connection);
    }
    tvdbEpisode.seasonNumber.changeValue(season);
  }

  private Optional<TVDBEpisode> findExistingTVDBEpisode() {
    int tvdbEpisodeExtId = episodeJson.getInt("id");
    Optional<TVDBEpisode> existingTVDBEpisodeByTVDBID = findExistingTVDBEpisodeByTVDBID(tvdbEpisodeExtId);
    if (existingTVDBEpisodeByTVDBID.isPresent()) {
      return existingTVDBEpisodeByTVDBID;
    } else {
      @NotNull Integer episodeNumber = jsonReader.getIntegerWithKey(episodeJson, "airedEpisodeNumber");
      @NotNull Integer seasonNumber = jsonReader.getIntegerWithKey(episodeJson, "airedSeason");

      Optional<TVDBEpisode> existingTVDBEpisodeByEpisodeNumber = findExistingTVDBEpisodeByEpisodeNumber(episodeNumber, seasonNumber);
      if (existingTVDBEpisodeByEpisodeNumber.isPresent()) {
        return existingTVDBEpisodeByEpisodeNumber;
      }
    }

    return Optional.empty();
  }

  private Optional<TVDBEpisode> findExistingTVDBEpisodeByTVDBID(Integer tvdbEpisodeExtId) {
    return tvdbEpisodes.stream()
        .filter(tvdbEpisode -> tvdbEpisodeExtId.equals(tvdbEpisode.tvdbEpisodeExtId.getValue()))
        .findFirst();
  }

  private Optional<TVDBEpisode> findExistingTVDBEpisodeByEpisodeNumber(Integer episodeNumber, Integer seasonNumber) {
    return tvdbEpisodes.stream()
        .filter(tvdbEpisode -> matchesByEpisodeNumber(tvdbEpisode, episodeNumber, seasonNumber, series.tvdbSeriesId.getValue()))
        .findFirst();
  }

  private Boolean matchesByEpisodeNumber(TVDBEpisode tvdbEpisode, Integer episodeNumber, Integer seasonNumber, Integer tvdbSeriesId) {
    return episodeNumber.equals(tvdbEpisode.episodeNumber.getValue()) &&
        seasonNumber.equals(tvdbEpisode.seasonNumber.getValue()) &&
        tvdbSeriesId.equals(tvdbEpisode.tvdbSeriesId.getValue());
  }

  private Episode getEpisodeFromTVDBEpisodeID(Integer tvdbEpisodeID) {
    Optional<Episode> optionalEpisode = episodes.stream()
        .filter(episode -> tvdbEpisodeID.equals(episode.tvdbEpisodeId.getValue()))
        .findFirst();

    if (optionalEpisode.isPresent()) {
      return optionalEpisode.get();
    } else {
      throw new IllegalStateException("Found tvdbEpisode (ID " + tvdbEpisodeID + ") with no corresponding episode.");
    }
  }

}
