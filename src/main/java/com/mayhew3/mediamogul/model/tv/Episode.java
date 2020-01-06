package com.mayhew3.mediamogul.model.tv;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.mayhew3.mediamogul.model.tv.group.TVGroupEpisode;
import com.mayhew3.postgresobject.dataobject.*;
import com.mayhew3.postgresobject.db.SQLConnection;
import com.mayhew3.mediamogul.tv.exception.ShowFailedException;
import com.mayhew3.mediamogul.tv.TVDBMatchStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class Episode extends RetireableDataObject {

  /* Foreign Keys */
  public FieldValueForeignKey tvdbEpisodeId = registerForeignKey(new TVDBEpisode(), Nullability.NOT_NULL);
  public FieldValueForeignKey seasonId = registerForeignKey(new Season(), Nullability.NULLABLE);
  public FieldValueForeignKey seriesId = registerForeignKey(new Series(), Nullability.NOT_NULL);


  /* Data */
  public FieldValue<Integer> season = registerIntegerField("season", Nullability.NOT_NULL);
  public FieldValue<Integer> episodeNumber = registerIntegerField("episode_number", Nullability.NOT_NULL);
  public FieldValue<Integer> absoluteNumber = registerIntegerField("absolute_number", Nullability.NULLABLE);

  public FieldValueTimestamp airDate = registerTimestampField("air_date", Nullability.NULLABLE);
  public FieldValueTimestamp airTime = registerTimestampField("air_time", Nullability.NULLABLE);

  public FieldValue<Boolean> onTiVo = registerBooleanField("on_tivo", Nullability.NOT_NULL).defaultValue(false);

  public FieldValueString title = registerStringField("title", Nullability.NULLABLE);
  public FieldValueString seriesTitle = registerStringField("series_title", Nullability.NULLABLE);

  /* User Data */
  public FieldValueTimestamp watchedDate = registerTimestampField("watched_date", Nullability.NULLABLE);
  public FieldValue<Boolean> watched = registerBooleanField("watched", Nullability.NOT_NULL).defaultValue(false);
  public FieldValueBoolean streaming = registerBooleanField("streaming", Nullability.NOT_NULL).defaultValue(false);

  public FieldValueTimestamp lastTVDBError = registerTimestampField("last_tvdb_error", Nullability.NULLABLE);

  public Episode() {
    registerTimestampField("last_tvdb_update", Nullability.NULLABLE);
    registerTimestampField("last_tvdb_sanity_check", Nullability.NULLABLE);
    addColumnsIndex(seriesId, retired);
  }

  @Override
  public String getTableName() {
    return "episode";
  }

  @Override
  protected String createDDLStatement() {
    return super.createDDLStatement();
  }

  @Override
  public String toString() {
    return seriesTitle.getValue() + " " + season.getValue() + "x" + episodeNumber.getValue() + ": '" + title.getValue() + "'";
  }

  public void addToTiVoEpisodes(SQLConnection connection, @NotNull TiVoEpisode tiVoEpisode) throws SQLException {
    List<TiVoEpisode> tiVoEpisodes = getTiVoEpisodes(connection);
    if (!hasMatch(tiVoEpisodes, tiVoEpisode.id.getValue())) {
      EdgeTiVoEpisode edgeTiVoEpisode = new EdgeTiVoEpisode();
      edgeTiVoEpisode.initializeForInsert();

      edgeTiVoEpisode.tivoEpisodeId.changeValue(tiVoEpisode.id.getValue());
      edgeTiVoEpisode.episodeId.changeValue(id.getValue());

      edgeTiVoEpisode.commit(connection);

      tiVoEpisode.tvdbMatchStatus.changeValue(TVDBMatchStatus.MATCH_COMPLETED);
      tiVoEpisode.commit(connection);
    }

    onTiVo.changeValue(true);
    commit(connection);
  }

  public Boolean hasMatch(List<TiVoEpisode> tiVoEpisodes, Integer tivoLocalEpisodeId) {
    for (TiVoEpisode tiVoEpisode : tiVoEpisodes) {
      if (tivoLocalEpisodeId.equals(tiVoEpisode.id.getValue())) {
        return true;
      }
    }
    return false;
  }

  public List<TiVoEpisode> getTiVoEpisodes(SQLConnection connection) throws SQLException {
    String sql = "SELECT te.* " +
        "FROM tivo_episode te " +
        "INNER JOIN edge_tivo_episode e " +
        "  ON e.tivo_episode_id = te.id " +
        "WHERE e.episode_id = ? " +
        "AND te.retired = ?";
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, id.getValue(), 0);
    List<TiVoEpisode> tiVoEpisodeList = new ArrayList<>();

    while (resultSet.next()) {
      TiVoEpisode tiVoEpisode = new TiVoEpisode();
      tiVoEpisode.initializeFromDBObject(resultSet);

      tiVoEpisodeList.add(tiVoEpisode);
    }

    return tiVoEpisodeList;
  }

  public TVDBEpisode getTVDBEpisode(SQLConnection connection) throws SQLException, ShowFailedException {
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(
        "SELECT * " +
            "FROM tvdb_episode " +
            "WHERE id = ? " +
            "AND retired = ?", tvdbEpisodeId.getValue(), 0);

    if (resultSet.next()) {
      TVDBEpisode tvdbEpisode = new TVDBEpisode();
      tvdbEpisode.initializeFromDBObject(resultSet);
      return tvdbEpisode;
    } else {
      throw new ShowFailedException("Episode " + id.getValue() + " has tvdb_episode_id " + tvdbEpisodeId.getValue() + " that wasn't found.");
    }
  }

  public List<EpisodeRating> getEpisodeRatings(SQLConnection connection) throws SQLException {
    String sql = "select * " +
        "from episode_rating " +
        "where episode_id = ? " +
        "and retired = ? " +
        "order by id ";

    List<EpisodeRating> episodeRatings = new ArrayList<>();

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, id.getValue(), 0);

    while (resultSet.next()) {
      EpisodeRating episodeRating = new EpisodeRating();
      episodeRating.initializeFromDBObject(resultSet);

      episodeRatings.add(episodeRating);
    }
    return episodeRatings;
  }

  public List<TVGroupEpisode> getTVGroupEpisodes(SQLConnection connection) throws SQLException {
    String sql = "select * " +
        "from tv_group_episode " +
        "where episode_id = ? " +
        "and retired = ? ";

    List<TVGroupEpisode> tvGroupEpisodes = new ArrayList<>();

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, id.getValue(), 0);

    while (resultSet.next()) {
      TVGroupEpisode tvGroupEpisode = new TVGroupEpisode();
      tvGroupEpisode.initializeFromDBObject(resultSet);

      tvGroupEpisodes.add(tvGroupEpisode);
    }
    return tvGroupEpisodes;
  }


  public Series getSeries(SQLConnection connection) throws SQLException, ShowFailedException {
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch("SELECT * FROM series WHERE id = ?", seriesId.getValue());

    if (resultSet.next()) {
      Series series = new Series();
      series.initializeFromDBObject(resultSet);
      return series;
    } else {
      throw new ShowFailedException("Episode " + id.getValue() + " has series_id " + seriesId.getValue() + " that wasn't found.");
    }
  }

  public Integer getSeason() {
    return season.getValue();
  }

  public void setSeason(Integer seasonNumber, SQLConnection connection) throws SQLException {
    season.changeValue(seasonNumber);
    updateSeasonRow(season.getValue(), connection);
  }

  public void setSeasonFromString(String seasonNumber, SQLConnection connection) throws SQLException {
    season.changeValueFromString(seasonNumber);
    updateSeasonRow(season.getValue(), connection);
  }


  public void updateAirTime(String seriesAirTime) {
    if (seriesAirTime == null) {
      seriesAirTime = "00:00";
    }

    // my first-time update populated this for ALL rows with air date, but periodic updates
    // should only populate new episodes with future dates using series time. Because most
    // commonly the air time will refer to CURRENT episodes, not past ones.
    if (airDate.getValue() == null) {
      airTime.changeValue(null);
    } else if (futureAirDateOrFirstAirTime(airDate.getValue())) {

      DateTimeFormatter dateOnlyFormatter = DateTimeFormat.forPattern("yyyy-MM-dd");
      String dateOnly = dateOnlyFormatter.print(airDate.getValue().getTime());

      String stringWithTime = dateOnly + " " + seriesAirTime;

      List<String> possibleTimeFormats = Lists.newArrayList(
          "hh:mm aa",
          "hh:mmaa",
          "HH:mm",
          "h aa",
          "haa",
          "HHmm",
          "h.mmaa",
          "hh:mm aa zzz"
      );

      Optional<Date> airTimeDate = tryToParse(stringWithTime, possibleTimeFormats);
      airTimeDate.ifPresent(date -> airTime.changeValue(date));
    }
  }

  private boolean futureAirDateOrFirstAirTime(Timestamp airDate) {
    return airTime.getValue() == null || isInFuture(airTime.getValue()) ||
        airDate == null || isInFuture(airDate);
  }

  private Boolean isInFuture(Timestamp airDate) {
    Date date = new Date(airDate.getTime());
    Date today = trimToMidnight(new Date());

    return !today.after(trimToMidnight(date));
  }

  private Date trimToMidnight(Date date) {
    return new DateTime(date).withTimeAtStartOfDay().toDate();
  }

  private Optional<Date> tryToParse(String stringWithTime, List<String> minuteFormats) {
    for (String minuteFormat : minuteFormats) {
      DateTimeFormatter simpleDateFormat = DateTimeFormat.forPattern("yyyy-MM-dd " + minuteFormat);
      try {
        DateTime dateTime = simpleDateFormat.parseDateTime(stringWithTime);
        return Optional.of(dateTime.toDate());
      } catch (IllegalArgumentException ignored) {
      }
    }
    return Optional.empty();
  }

  private void updateSeasonRow(Integer seasonNumber, SQLConnection connection) throws SQLException {
    Preconditions.checkState(seriesId.getValue() != null, "Can't update the season if there is no associated series_id yet.");

    if (seasonId.getValue() == null) {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(
          "SELECT * " +
              "FROM season s " +
              "WHERE s.series_id = ? " +
              "AND s.season_number = ? ",
          seriesId.getValue(), seasonNumber);

      Season seasonObject = new Season();
      if (resultSet.next()) {
        seasonObject.initializeFromDBObject(resultSet);
      } else {
        seasonObject.initializeForInsert();
        seasonObject.dateModified.changeValue(new Date());
        seasonObject.seasonNumber.changeValue(seasonNumber);
        seasonObject.seriesId.changeValue(seriesId.getValue());

        seasonObject.commit(connection);
      }
      seasonId.changeValue(seasonObject.id.getValue());
    }
  }

  public EpisodeRating getMostRecentRating(SQLConnection connection, @NotNull Timestamp ratingCutoff) throws SQLException {
    return getMostRecentRating(connection, Optional.of(ratingCutoff));
  }

  public EpisodeRating getMostRecentRating(SQLConnection connection) throws SQLException {
    return getMostRecentRating(connection, Optional.empty());
  }

  @Nullable
  private EpisodeRating getMostRecentRating(SQLConnection connection, Optional<Timestamp> ratingCutoff) throws SQLException {
    String cutoffClause = ratingCutoff.isPresent() ? "and date_added < ? " : "";

    String sql = "select *\n" +
        "from episode_rating\n" +
        "where episode_id = ?\n" +
        "and person_id = ? " +
        cutoffClause +
        "order by date_added desc";

    ResultSet resultSet = ratingCutoff.isPresent() ?
        connection.prepareAndExecuteStatementFetch(sql, id.getValue(), 1, ratingCutoff.get()) :
        connection.prepareAndExecuteStatementFetch(sql, id.getValue(), 1);

    if (resultSet.next()) {
      EpisodeRating episodeRating = new EpisodeRating();
      episodeRating.initializeFromDBObject(resultSet);

      return episodeRating;
    }

    return null;
  }
}
