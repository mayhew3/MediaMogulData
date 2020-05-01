package com.mayhew3.mediamogul.model.tv;

import com.google.common.base.Preconditions;
import com.mayhew3.postgresobject.dataobject.*;
import com.mayhew3.postgresobject.db.SQLConnection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

public class TVDBSeries extends RetireableDataObject {

  public FieldValueTimestamp firstAired = registerTimestampField("first_aired", Nullability.NULLABLE);

  public FieldValueInteger tvdbSeriesExtId = registerIntegerField("tvdb_series_ext_id", Nullability.NULLABLE);

  public FieldValueInteger ratingCount = registerIntegerField("rating_count", Nullability.NULLABLE);
  public FieldValueInteger runtime = registerIntegerField("runtime", Nullability.NULLABLE);
  public FieldValueBigDecimal rating = registerBigDecimalField("rating", Nullability.NULLABLE);
  public FieldValue<String> name = registerStringField("name", Nullability.NULLABLE);

  public FieldValue<String> airsDayOfWeek = registerStringField("airs_day_of_week", Nullability.NULLABLE);
  public FieldValue<String> airsTime = registerStringField("airs_time", Nullability.NULLABLE);
  public FieldValue<String> network = registerStringField("network", Nullability.NULLABLE);
  public FieldValue<String> overview = registerStringField("overview", Nullability.NULLABLE);
  public FieldValue<String> status = registerStringField("status", Nullability.NULLABLE);
  public FieldValue<String> lastPoster = registerStringField("last_poster", Nullability.NULLABLE);
  public FieldValue<String> firstPoster = registerStringField("first_poster", Nullability.NULLABLE);
  public FieldValue<String> banner = registerStringField("banner", Nullability.NULLABLE);
  public FieldValue<String> lastUpdated = registerStringField("last_updated", Nullability.NULLABLE);
  public FieldValue<String> imdbId = registerStringField("imdb_id", Nullability.NULLABLE);
  public FieldValue<String> zap2it_id = registerStringField("zap2it_id", Nullability.NULLABLE);

  public FieldValueInteger apiVersion = registerIntegerField("api_version", Nullability.NOT_NULL).defaultValue(1);

  public TVDBSeries() {
    super();
    addUniqueConstraint(tvdbSeriesExtId);
  }

  @Override
  public String getTableName() {
    return "tvdb_series";
  }

  @Override
  public String toString() {
    return name.getValue();
  }

  public Optional<TVDBPoster> addPosterIfDoesntExist(String posterPath, @Nullable Integer season, SQLConnection connection) throws SQLException {
    Preconditions.checkNotNull(id.getValue(), "Cannot insert join entity until TVDBSeries object is committed (id is non-null)");

    @NotNull ResultSet resultSet = connection.prepareAndExecuteStatementFetch("SELECT 1 " +
            "FROM tvdb_poster " +
            "WHERE poster_path = ? " +
            "AND tvdb_series_id = ? " +
            "AND retired = ? ",
        posterPath, id.getValue(), 0);
    if (!resultSet.next()) {
      TVDBPoster tvdbPoster = new TVDBPoster();
      tvdbPoster.initializeForInsert();
      tvdbPoster.posterPath.changeValue(posterPath);
      tvdbPoster.tvdb_series_id.changeValue(id.getValue());
      tvdbPoster.season.changeValue(season);
      tvdbPoster.commit(connection);

      return Optional.of(tvdbPoster);
    }
    return Optional.empty();
  }
}
