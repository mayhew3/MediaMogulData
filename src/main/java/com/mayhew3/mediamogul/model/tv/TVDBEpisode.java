package com.mayhew3.mediamogul.model.tv;

import com.mayhew3.postgresobject.dataobject.*;
import com.mayhew3.postgresobject.db.SQLConnection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.ResultSet;
import java.sql.SQLException;

public class TVDBEpisode extends RetireableDataObject {
  /* Foreign Keys */
  public FieldValueForeignKey tvdbSeriesId = registerForeignKey(new TVDBSeries(), Nullability.NOT_NULL);

  public FieldValue<Integer> seasonNumber = registerIntegerField("season_number", Nullability.NOT_NULL);

  public FieldValue<Integer> tvdbSeasonExtId = registerIntegerField("tvdb_season_ext_id", Nullability.NULLABLE);

  public FieldValue<Integer> tvdbEpisodeExtId = registerIntegerField("tvdb_episode_ext_id", Nullability.NULLABLE);
  public FieldValue<Integer> episodeNumber = registerIntegerField("episode_number", Nullability.NOT_NULL);
  public FieldValue<Integer> absoluteNumber = registerIntegerField("absolute_number", Nullability.NULLABLE);
  public FieldValue<Integer> ratingCount = registerIntegerField("rating_count", Nullability.NULLABLE);
  public FieldValue<Integer> airsAfterSeason = registerIntegerField("airs_after_season", Nullability.NULLABLE);
  public FieldValue<Integer> airsBeforeSeason = registerIntegerField("airs_before_season", Nullability.NULLABLE);
  public FieldValue<Integer> airsBeforeEpisode = registerIntegerField("airs_before_episode", Nullability.NULLABLE);
  public FieldValue<Integer> thumbHeight = registerIntegerField("thumb_height", Nullability.NULLABLE);
  public FieldValue<Integer> thumbWidth = registerIntegerField("thumb_width", Nullability.NULLABLE);

  public FieldValueTimestamp firstAired = registerTimestampField("first_aired", Nullability.NULLABLE);
  public FieldValue<Integer> lastUpdated = registerIntegerField("last_updated", Nullability.NULLABLE);

  public FieldValueBigDecimal rating = registerBigDecimalField("rating", Nullability.NULLABLE);


  public FieldValue<String> seriesName = registerStringField("series_name", Nullability.NULLABLE);
  public FieldValue<String> name = registerStringField("name", Nullability.NULLABLE);
  public FieldValue<String> overview = registerStringField("overview", Nullability.NULLABLE);
  public FieldValue<String> productionCode = registerStringField("production_code", Nullability.NULLABLE);
  public FieldValue<String> director = registerStringField("director", Nullability.NULLABLE);
  public FieldValue<String> writer = registerStringField("writer", Nullability.NULLABLE);
  public FieldValue<String> filename = registerStringField("filename", Nullability.NULLABLE);

  public FieldValueInteger apiVersion = registerIntegerField("api_version", Nullability.NOT_NULL).defaultValue(1);

  public TVDBEpisode() {
    addUniqueConstraint(tvdbEpisodeExtId, retired);
  }

  @Override
  public String getTableName() {
    return "tvdb_episode";
  }

  @Override
  public String toString() {
    return seasonNumber.getValue() + "x" + episodeNumber.getValue() + ": '" + name.getValue() + "'";
  }

  @NotNull
  public Episode getEpisode(SQLConnection connection) throws SQLException {
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(
        "SELECT * " +
            "FROM episode " +
            "WHERE tvdb_episode_id = ? " +
            "AND retired = ?",
        id.getValue(),
        0
    );

    if (!resultSet.next()) {
      throw new IllegalStateException("No episode found with tvdb_episode_id of " + id.getValue());
    }
    Episode episode = new Episode();
    episode.initializeFromDBObject(resultSet);
    return episode;
  }

  @Nullable
  public Episode getEpisodeOrNull(SQLConnection connection) throws SQLException {
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(
        "SELECT * " +
            "FROM episode " +
            "WHERE tvdb_episode_id = ? " +
            "AND retired = ?",
        id.getValue(),
        0
    );

    if (!resultSet.next()) {
      return null;
    }
    Episode episode = new Episode();
    episode.initializeFromDBObject(resultSet);
    return episode;
  }
}
