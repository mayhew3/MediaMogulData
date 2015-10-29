package com.mayhew3.gamesutil.mediaobject;

import com.google.common.base.Preconditions;
import com.mayhew3.gamesutil.games.PostgresConnection;
import com.sun.istack.internal.Nullable;

import java.sql.ResultSet;
import java.sql.SQLException;

public class SeriesPostgres extends MediaObjectPostgreSQL {

  /* Foreign Keys */
  public FieldValueInteger tvdbSeriesId = registerIntegerField("tvdb_series_id");

  /* Data */
  public FieldValueString seriesTitle = registerStringField("title");
  public FieldValueInteger tier = registerIntegerField("tier");
  public FieldValueInteger metacritic = registerIntegerField("metacritic");
  public FieldValueString tivoSeriesId = registerStringField("tivo_series_id");
  public FieldValueInteger tvdbId = registerIntegerField("tvdb_id");

  /* Matching Helpers */
  public FieldValueString metacriticHint = registerStringField("metacritic_hint");
  public FieldValueBoolean ignoreTVDB = registerBooleanField("ignore_tvdb");
  public FieldValueBoolean matchedWrong = registerBooleanField("matched_wrong");
  public FieldValueBoolean needsTVDBRedo = registerBooleanField("needs_tvdb_redo");
  public FieldValueString tvdbHint = registerStringField("tvdb_hint");

  /* Denorms */
  public FieldValueInteger activeEpisodes = registerIntegerField("active_episodes");
  public FieldValueInteger deletedEpisodes = registerIntegerField("deleted_episodes");
  public FieldValueInteger suggestionEpisodes = registerIntegerField("suggestion_episodes");
  public FieldValueInteger unmatchedEpisodes = registerIntegerField("unmatched_episodes");
  public FieldValueInteger watchedEpisodes = registerIntegerField("watched_episodes");
  public FieldValueInteger unwatchedEpisodes = registerIntegerField("unwatched_episodes");
  public FieldValueInteger unwatchedUnrecorded = registerIntegerField("unwatched_unrecorded");
  public FieldValueInteger tvdbOnlyEpisodes = registerIntegerField("tvdb_only_episodes");
  public FieldValueInteger matchedEpisodes = registerIntegerField("matched_episodes");

  public FieldValueTimestamp lastUnwatched = registerTimestampField("last_unwatched");
  public FieldValueTimestamp mostRecent = registerTimestampField("most_recent");
  public FieldValueBoolean isSuggestion = registerBooleanField("suggestion");


  @Override
  protected String getTableName() {
    return "series";
  }

  @Override
  public String toString() {
    return seriesTitle.getValue();
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

    ignoreTVDB.changeValue(false);
    isSuggestion.changeValue(false);
    needsTVDBRedo.changeValue(false);
    matchedWrong.changeValue(false);
  }

  /**
   * @param connection DB connection to use
   * @param genreName Name of new or existing genre
   * @return New SeriesGenrePostgres join entity, if a new one was created. Null otherwise.
   * @throws SQLException
   */
  @Nullable
  public SeriesGenrePostgres addGenre(PostgresConnection connection, String genreName) throws SQLException {
    Preconditions.checkNotNull(id.getValue(), "Cannot insert join entity until Series object is committed (id is non-null)");

    GenrePostgres genrePostgres = GenrePostgres.findOrCreate(connection, genreName);

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(
        "SELECT * FROM series_genre WHERE series_id = ? AND genre_id = ?",
        id.getValue(),
        genrePostgres.id.getValue());

    if (!connection.hasMoreElements(resultSet)) {
      SeriesGenrePostgres seriesGenrePostgres = new SeriesGenrePostgres();
      seriesGenrePostgres.initializeForInsert();

      seriesGenrePostgres.seriesId.changeValue(id.getValue());
      seriesGenrePostgres.genreId.changeValue(genrePostgres.id.getValue());

      seriesGenrePostgres.commit(connection);
      return seriesGenrePostgres;
    }

    return null;
  }

  /**
   * @param connection DB connection to use
   * @param viewingLocationName Name of new or existing viewing location
   * @return New {{@link}SeriesViewingLocationPostgres} join entity, if a new one was created. Null otherwise.
   * @throws SQLException
   */
  @Nullable
  public SeriesViewingLocationPostgres addViewingLocation(PostgresConnection connection, String viewingLocationName) throws SQLException {
    Preconditions.checkNotNull(id.getValue(), "Cannot insert join entity until Series object is committed (id is non-null)");

    ViewingLocationPostgres viewingLocationPostgres = ViewingLocationPostgres.findOrCreate(connection, viewingLocationName);

    SeriesViewingLocationPostgres seriesViewingLocationPostgres = new SeriesViewingLocationPostgres();

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(
        "SELECT * FROM " + seriesViewingLocationPostgres.getTableName() + " " +
            "WHERE " + seriesViewingLocationPostgres.seriesId.getFieldName() + " = ? " +
            "AND " + seriesViewingLocationPostgres.viewingLocationId.getFieldName() + " = ?",
        id.getValue(),
        viewingLocationPostgres.id.getValue());

    if (!connection.hasMoreElements(resultSet)) {
      seriesViewingLocationPostgres.initializeForInsert();

      seriesViewingLocationPostgres.seriesId.changeValue(id.getValue());
      seriesViewingLocationPostgres.viewingLocationId.changeValue(viewingLocationPostgres.id.getValue());

      seriesViewingLocationPostgres.commit(connection);
      return seriesViewingLocationPostgres;
    }

    return null;
  }
}
