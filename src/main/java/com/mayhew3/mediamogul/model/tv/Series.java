package com.mayhew3.mediamogul.model.tv;

import com.google.common.base.Preconditions;
import com.mayhew3.postgresobject.dataobject.*;
import com.mayhew3.postgresobject.db.SQLConnection;
import com.mayhew3.mediamogul.model.Person;
import com.mayhew3.mediamogul.tv.TVDBMatchStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class Series extends RetireableDataObject implements Comparable<Series> {

  /* Foreign Keys */
  public FieldValueForeignKey tvdbSeriesId = registerForeignKey(new TVDBSeries(), Nullability.NULLABLE);

  /* Data */
  public FieldValueString seriesTitle = registerStringField("title", Nullability.NULLABLE);
  public FieldValueInteger tier = registerIntegerField("tier", Nullability.NULLABLE, IntegerSize.SMALLINT);
  public FieldValueInteger metacritic = registerIntegerField("metacritic", Nullability.NULLABLE, IntegerSize.SMALLINT);
  public FieldValueInteger mayhewRating = registerIntegerField("mayhew_rating", Nullability.NULLABLE, IntegerSize.SMALLINT);

  public FieldValueString tivoSeriesExtId = registerStringField("tivo_series_ext_id", Nullability.NULLABLE);
  public FieldValueString tivoSeriesV2ExtId = registerStringField("tivo_series_v2_ext_id", Nullability.NULLABLE);
  public FieldValueInteger tvdbSeriesExtId = registerIntegerField("tvdb_series_ext_id", Nullability.NULLABLE);

  public FieldValueString poster = registerStringField("poster", Nullability.NULLABLE);
  public FieldValueString cloud_poster = registerStringField("cloud_poster", Nullability.NULLABLE);
  public FieldValueString airTime = registerStringField("air_time", Nullability.NULLABLE);

  /* Matching Helpers */
  public FieldValueString metacriticHint = registerStringField("metacritic_hint", Nullability.NULLABLE);
  public FieldValueBoolean ignoreTVDB = registerBooleanField("ignore_tvdb", Nullability.NOT_NULL).defaultValue(false);
  public FieldValueBoolean matchedWrong = registerBooleanField("matched_wrong", Nullability.NOT_NULL).defaultValue(false);
  public FieldValueBoolean needsTVDBRedo = registerBooleanField("needs_tvdb_redo", Nullability.NOT_NULL).defaultValue(false);
  public FieldValueString tvdbHint = registerStringField("tvdb_hint", Nullability.NULLABLE);
  public FieldValueString tivoName = registerStringField("tivo_name", Nullability.NULLABLE);
  public FieldValueInteger tvdbMatchId = registerIntegerField("tvdb_match_id", Nullability.NULLABLE);


  /* Denorms */
  public FieldValueInteger activeEpisodes = registerIntegerField("active_episodes", Nullability.NOT_NULL).defaultValue(0);
  public FieldValueInteger deletedEpisodes = registerIntegerField("deleted_episodes", Nullability.NOT_NULL).defaultValue(0);
  public FieldValueInteger suggestionEpisodes = registerIntegerField("suggestion_episodes", Nullability.NOT_NULL).defaultValue(0);
  public FieldValueInteger unmatchedEpisodes = registerIntegerField("unmatched_episodes", Nullability.NOT_NULL).defaultValue(0);
  public FieldValueInteger watchedEpisodes = registerIntegerField("watched_episodes", Nullability.NOT_NULL).defaultValue(0);
  public FieldValueInteger unwatchedEpisodes = registerIntegerField("unwatched_episodes", Nullability.NOT_NULL).defaultValue(0);
  public FieldValueInteger unwatchedUnrecorded = registerIntegerField("unwatched_unrecorded", Nullability.NOT_NULL).defaultValue(0);
  public FieldValueInteger tvdbOnlyEpisodes = registerIntegerField("tvdb_only_episodes", Nullability.NOT_NULL).defaultValue(0);
  public FieldValueInteger matchedEpisodes = registerIntegerField("matched_episodes", Nullability.NOT_NULL).defaultValue(0);
  public FieldValueInteger streamingEpisodes = registerIntegerField("streaming_episodes", Nullability.NOT_NULL).defaultValue(0);
  public FieldValueInteger unwatchedStreaming = registerIntegerField("unwatched_streaming", Nullability.NOT_NULL).defaultValue(0);

  public FieldValueTimestamp firstUnwatched = registerTimestampField("first_unwatched", Nullability.NULLABLE);
  public FieldValueTimestamp lastUnwatched = registerTimestampField("last_unwatched", Nullability.NULLABLE);
  public FieldValueTimestamp mostRecent = registerTimestampField("most_recent", Nullability.NULLABLE);
  public FieldValueBoolean isSuggestion = registerBooleanField("suggestion", Nullability.NOT_NULL).defaultValue(false);

  public FieldValueBoolean tvdbNew = registerBooleanField("tvdb_new", Nullability.NOT_NULL).defaultValue(true);
  public FieldValueBoolean metacriticNew = registerBooleanField("metacritic_new", Nullability.NOT_NULL).defaultValue(true);

  public FieldValueInteger tivoVersion = registerIntegerField("tivo_version", Nullability.NOT_NULL).defaultValue(1);

  public FieldValueTimestamp lastTVDBUpdate = registerTimestampField("last_tvdb_update", Nullability.NULLABLE);
  public FieldValueTimestamp lastTVDBError = registerTimestampField("last_tvdb_error", Nullability.NULLABLE);
  public FieldValueTimestamp lastTVDBSanityCheck = registerTimestampField("last_tvdb_sanity_check", Nullability.NULLABLE);

  public FieldValueString tvdbMatchStatus = registerStringField("tvdb_match_status", Nullability.NOT_NULL).defaultValue(TVDBMatchStatus.MATCH_FIRST_PASS);
  public FieldValueTimestamp tvdbConfirmDate = registerTimestampField("tvdb_confirm_date", Nullability.NULLABLE);
  public FieldValueTimestamp tvdbIgnoreDate = registerTimestampField("tvdb_ignore_date", Nullability.NULLABLE);
  public FieldValueBoolean tvdbManualQueue = registerBooleanField("tvdb_manual_queue", Nullability.NOT_NULL).defaultValue(Boolean.FALSE);

  public FieldValueInteger consecutiveTVDBErrors = registerIntegerField("consecutive_tvdb_errors", Nullability.NOT_NULL).defaultValue(0);

  public FieldValueString addedBy = registerStringField("added_by", Nullability.NOT_NULL).defaultValue("Manual");
  public FieldValueForeignKey addedByUser = registerForeignKey(new Person(), Nullability.NULLABLE);

  public Series() {
    registerStringField("trailer_link", Nullability.NULLABLE);

    addUniqueConstraint(tvdbSeriesExtId);
  }

  @Override
  public String getTableName() {
    return "series";
  }

  @Override
  public String toString() {
    String idString = id.getValue() == null ? "" : id.getValue().toString();
    return seriesTitle.getValue() + " (" + idString + ")";
  }

  public void initializeDenorms() {
    activeEpisodes.changeValue(0);
    deletedEpisodes.changeValue(0);
    suggestionEpisodes.changeValue(0);
    unmatchedEpisodes.changeValue(0);
    watchedEpisodes.changeValue(0);
    unwatchedEpisodes.changeValue(0);
    unwatchedUnrecorded.changeValue(0);
    tvdbOnlyEpisodes.changeValue(0);
    matchedEpisodes.changeValue(0);
    streamingEpisodes.changeValue(0);
    unwatchedStreaming.changeValue(0);

    ignoreTVDB.changeValue(false);
    isSuggestion.changeValue(false);
    needsTVDBRedo.changeValue(false);
    matchedWrong.changeValue(false);
  }

  public static Optional<Series> findSeriesFromTitle(String seriesTitle, SQLConnection connection) throws SQLException {
    String sql = "SELECT * " +
        "FROM series " +
        "WHERE title = ? " +
        "AND retired = ? ";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, seriesTitle, 0);

    if (resultSet.next()) {
      Series series = new Series();
      series.initializeFromDBObject(resultSet);
      return Optional.of(series);
    } else {
      return Optional.empty();
    }
  }

  public static Optional<Series> findSeriesFromTVDBExtID(Integer tvdbSeriesExtId, SQLConnection connection) throws SQLException {
    String sql = "SELECT * " +
        "FROM series " +
        "WHERE tvdb_series_ext_id = ? " +
        "AND retired = ? ";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, tvdbSeriesExtId, 0);

    if (resultSet.next()) {
      Series series = new Series();
      series.initializeFromDBObject(resultSet);
      return Optional.of(series);
    } else {
      return Optional.empty();
    }
  }

  /**
   * @param connection DB connection to use
   * @param genreName Name of new or existing genre
   * @return New SeriesGenrePostgres join entity, if a new one was created. Null otherwise.
   * @throws SQLException
   */
  @Nullable
  public SeriesGenre addGenre(SQLConnection connection, String genreName) throws SQLException {
    Preconditions.checkNotNull(id.getValue(), "Cannot insert join entity until Series object is committed (id is non-null)");

    Genre genre = Genre.findOrCreate(connection, genreName);

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(
        "SELECT * FROM series_genre WHERE series_id = ? AND genre_id = ?",
        id.getValue(),
        genre.id.getValue());

    if (!resultSet.next()) {
      SeriesGenre seriesGenre = new SeriesGenre();
      seriesGenre.initializeForInsert();

      seriesGenre.seriesId.changeValue(id.getValue());
      seriesGenre.genreId.changeValue(genre.id.getValue());

      seriesGenre.commit(connection);
      return seriesGenre;
    }

    return null;
  }

  public void addPossibleSeriesMatch(SQLConnection connection, PossibleSeriesMatch possibleSeriesMatch) throws SQLException {
    Preconditions.checkNotNull(id.getValue(), "Cannot insert child entity until Series object is committed (id is non-null)");

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(
        "SELECT * FROM possible_series_match " +
            "WHERE " + possibleSeriesMatch.tvdbSeriesExtId.getFieldName() + " = ? " +
            "and retired = ? ",
        possibleSeriesMatch.tvdbSeriesExtId.getValue(), 0);

    if (!resultSet.next()) {
      possibleSeriesMatch.seriesId.changeValue(id.getValue());
      possibleSeriesMatch.commit(connection);
    }
  }

  /**
   * @param connection DB connection to use
   * @param viewingLocationName Name of new or existing viewing location
   * @return New {{@link}SeriesViewingLocationPostgres} join entity, if a new one was created. Null otherwise.
   * @throws SQLException
   */
  @Nullable
  public SeriesViewingLocation addViewingLocation(SQLConnection connection, String viewingLocationName) throws SQLException {
    Preconditions.checkNotNull(id.getValue(), "Cannot insert join entity until Series object is committed (id is non-null)");

    ViewingLocation viewingLocation = ViewingLocation.findOrCreate(connection, viewingLocationName);

    SeriesViewingLocation seriesViewingLocation = new SeriesViewingLocation();

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(
        "SELECT * FROM " + seriesViewingLocation.getTableName() + " " +
            "WHERE " + seriesViewingLocation.seriesId.getFieldName() + " = ? " +
            "AND " + seriesViewingLocation.viewingLocationId.getFieldName() + " = ?",
        id.getValue(),
        viewingLocation.id.getValue());

    if (!resultSet.next()) {
      seriesViewingLocation.initializeForInsert();

      seriesViewingLocation.seriesId.changeValue(id.getValue());
      seriesViewingLocation.viewingLocationId.changeValue(viewingLocation.id.getValue());

      seriesViewingLocation.commit(connection);
      return seriesViewingLocation;
    }

    return null;
  }

  private List<ViewingLocation> getViewingLocations(SQLConnection connection) throws SQLException {
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(
        "SELECT vl.* " +
            "FROM viewing_location vl " +
            "INNER JOIN series_viewing_location svl " +
            " ON svl.viewing_location_id = vl.id " +
            "WHERE svl.series_id = ?",
        id.getValue()
    );

    List<ViewingLocation> viewingLocations = new ArrayList<>();
    while (resultSet.next()) {
      ViewingLocation viewingLocation = new ViewingLocation();
      viewingLocation.initializeFromDBObject(resultSet);
      viewingLocations.add(viewingLocation);
    }
    return viewingLocations;
  }

  public Boolean isStreaming(SQLConnection connection) throws SQLException {
    for (ViewingLocation viewingLocation : getViewingLocations(connection)) {
      if (viewingLocation.streaming.getValue()) {
        return true;
      }
    }
    return false;
  }

  @NotNull
  public Season getOrCreateSeason(SQLConnection connection, Integer seasonNumber) throws SQLException {
    Preconditions.checkNotNull(id.getValue(), "Cannot insert join entity until Series object is committed (id is non-null)");

    Season season = new Season();

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(
        "SELECT * FROM " + season.getTableName() + " " +
            "WHERE " + season.seriesId.getFieldName() + " = ? " +
            "AND " + season.seasonNumber.getFieldName() + " = ?",
        id.getValue(),
        seasonNumber);
    if (resultSet.next()) {
      season.initializeFromDBObject(resultSet);
    } else {
      season.initializeForInsert();
      season.seriesId.changeValue(id.getValue());
      season.seasonNumber.changeValue(seasonNumber);

      season.commit(connection);
    }

    return season;
  }

  @Override
  public boolean equals(Object obj) {
    Preconditions.checkState(obj instanceof Series, "Should only call equals on another Series");
    Series otherSeries = (Series) obj;
    return id.getValue().equals(otherSeries.id.getValue());
  }

  @NotNull
  public List<Episode> getEpisodes(SQLConnection connection) throws SQLException {
    List<Episode> episodes = new ArrayList<>();
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(
        "SELECT e.* " +
            "FROM episode e " +
            "WHERE e.series_id = ? " +
            "AND e.retired = ?", id.getValue(), 0);

    while (resultSet.next()) {
      Episode episode = new Episode();
      episode.initializeFromDBObject(resultSet);
      episodes.add(episode);
    }
    return episodes;
  }

  public Optional<TVDBSeries> getTVDBSeries(SQLConnection connection) throws SQLException {
    if (tvdbSeriesId.getValue() == null) {
      return Optional.empty();
    }
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(
        "SELECT * " +
            "FROM tvdb_series " +
            "WHERE id = ? " +
            "AND retired = ?", tvdbSeriesId.getValue(), 0
    );

    if (resultSet.next()) {
      TVDBSeries tvdbSeries = new TVDBSeries();
      tvdbSeries.initializeFromDBObject(resultSet);
      return Optional.of(tvdbSeries);
    } else {
      return Optional.empty();
    }
  }

  @NotNull
  public List<TiVoEpisode> getTiVoEpisodes(SQLConnection connection) throws SQLException {
    List<TiVoEpisode> tiVoEpisodes = new ArrayList<>();

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(
        "SELECT te.* " +
            "FROM tivo_episode te " +
            "WHERE te.series_title = ? " +
            "AND te.retired = ? ",
        tivoName.getValue(), 0
    );

    while (resultSet.next()) {
      TiVoEpisode tiVoEpisode = new TiVoEpisode();
      tiVoEpisode.initializeFromDBObject(resultSet);
      tiVoEpisodes.add(tiVoEpisode);
    }

    return tiVoEpisodes;
  }

  @Override
  public int compareTo(@NotNull Series o) {
    return id.getValue().compareTo(o.id.getValue());
  }
}
