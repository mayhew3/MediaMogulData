package com.mayhew3.mediamogul.tv;

import com.mayhew3.mediamogul.db.DatabaseEnvironments;
import com.mayhew3.mediamogul.model.tv.*;
import com.mayhew3.mediamogul.scheduler.UpdateRunner;
import com.mayhew3.mediamogul.tv.helper.UpdateMode;
import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
import com.mayhew3.postgresobject.exception.MissingEnvException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.joda.time.DateTime;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

import static java.lang.Math.toIntExact;

@SuppressWarnings({"OptionalIsPresent"})
public class EpisodeGroupUpdater implements UpdateRunner {

  private final SQLConnection connection;
  private final Integer year;

  private static final Logger logger = LogManager.getLogger(EpisodeGroupUpdater.class);

  public static void main(String... args) throws URISyntaxException, SQLException, MissingEnvException {
    ArgumentChecker argumentChecker = new ArgumentChecker(args);
    argumentChecker.addExpectedOption("year", false, "Override for year to run on.");

    Optional<String> yearStr = argumentChecker.getOptionalIdentifier("year");
    Integer year = yearStr.isPresent() ? Integer.parseInt(yearStr.get()) : null;

    SQLConnection connection = PostgresConnectionFactory.createConnection(DatabaseEnvironments.getEnvironmentForDBArgument(argumentChecker));

    EpisodeGroupUpdater updater = new EpisodeGroupUpdater(connection, year);
    updater.runUpdate();
  }

  public EpisodeGroupUpdater(SQLConnection connection, Integer year) {
    this.connection = connection;
    this.year = year;
  }

  @SuppressWarnings("SameParameterValue")
  public void runUpdate() throws SQLException {

    SystemVars systemVars = SystemVars.getSystemVars(connection);

    Integer currentYear = this.year == null ? systemVars.ratingYear.getValue() : this.year;
    Timestamp ratingEndDate = systemVars.ratingEndDate.getValue();

    String sql = "select s.* " +
        "from regular_episode e " +
        "inner join series s " +
        " on e.series_id = s.id " +
        "inner join episode_rating er " +
        " on er.episode_id = e.id " +
        "where e.air_date between ? and ? " +
        "and er.watched = ? " +
//        "and er.watched_date < ? " +
        "and er.retired = ? " +
        "and er.person_id = ? " +
        "group by s.id";

    Timestamp startDate = new Timestamp(beginningOfYear(currentYear).toDate().getTime());
    Timestamp endDate = new Timestamp(endOfYear(currentYear).toDate().getTime());

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, startDate, endDate, true, 0, 1);

    while (resultSet.next()) {
      Series series = new Series();
      series.initializeFromDBObject(resultSet);

      debug("Updating Series '" + series.seriesTitle.getValue() + "' (" + series.id.getValue() + ")");

      EpisodeGroupRating groupRating = getOrCreateExistingRatingForSeriesAndYear(series, currentYear);

      List<EpisodeInfo> airedEligible = getEligibleEpisodeInfos(groupRating);
      List<EpisodeInfo> watchedEligible = getWatchedEligibleEpisodeInfos(airedEligible, ratingEndDate);

      // AIRED WITHIN WINDOW

      groupRating.numEpisodes.changeValue(airedEligible.size());
      groupRating.aired.changeValue(getNumberOfAiredEpisodes(airedEligible));

      groupRating.lastAired.changeValue(getLastAired(airedEligible));
      groupRating.nextAirDate.changeValue(getNextAirDate(airedEligible));

      // WATCHED WITHIN WINDOW

      groupRating.watched.changeValue(getNumberOfWatchedEpisodes(watchedEligible));
      groupRating.rated.changeValue(getNumberOfRatedEpisodes(watchedEligible));

      groupRating.avgRating.changeValue(getAvgRating(watchedEligible));
      groupRating.maxRating.changeValue(getMaxRating(watchedEligible));
      groupRating.lastRating.changeValue(getLastRating(watchedEligible));

      groupRating.avgFunny.changeValue(getAvgFunny(watchedEligible));
      groupRating.avgCharacter.changeValue(getAvgCharacter(watchedEligible));
      groupRating.avgStory.changeValue(getAvgStory(watchedEligible));

      groupRating.postUpdateEpisodes.changeValue(getPostUpdateEpisodes(watchedEligible, groupRating.reviewUpdateDate.getValue()));

      // SUGGESTED RATING

      groupRating.suggestedRating.changeValue(getSuggestedRating(groupRating));

      debug(" - " + groupRating.rated.getValue() + "/" + groupRating.numEpisodes.getValue() + " ratings found, AVG: " + groupRating.avgRating.getValue());

      groupRating.commit(connection);
    }

  }

  private void debug(Object message) {
    logger.debug(message);
  }

  private Integer getNumberOfRatedEpisodes(List<EpisodeInfo> episodeInfos) {
    return episodeInfos.stream()
        .filter(episodeInfo -> (episodeInfo.episodeRating != null &&
            episodeInfo.episodeRating.ratingValue.getValue() != null
        ))
        .collect(Collectors.toList())
        .size();
  }

  private Integer getNumberOfWatchedEpisodes(List<EpisodeInfo> episodeInfos) {
    return episodeInfos.size();
  }

  private Integer getNumberOfAiredEpisodes(List<EpisodeInfo> episodeInfos) {
    Timestamp now = new Timestamp(new Date().getTime());
    return episodeInfos.stream()
        .filter(episodeInfo -> episodeInfo.episode.airTime.getValue().before(now))
        .collect(Collectors.toList())
        .size();
  }


  @Nullable
  private Timestamp getLastAired(List<EpisodeInfo> episodeInfos) {
    Comparator<EpisodeInfo> byAirDate = Comparator.comparing(a -> a.episode.airDate.getValue());
    Optional<Timestamp> first = episodeInfos.stream()
        .sorted(byAirDate.reversed())
        .map(episodeInfo -> episodeInfo.episode.airDate.getValue())
        .findFirst();
    return first.isPresent() ? first.get() : null;
  }

  @Nullable
  private Timestamp getNextAirDate(List<EpisodeInfo> episodeInfos) {
    Comparator<EpisodeInfo> byAirDate = Comparator.comparing(a -> a.episode.airDate.getValue());
    Optional<Timestamp> first = episodeInfos.stream()
        .filter(episodeInfo -> episodeInfo.episodeRating == null || !episodeInfo.episodeRating.watched.getValue())
        .sorted(byAirDate)
        .map(episodeInfo -> episodeInfo.episode.airDate.getValue())
        .findFirst();
    return first.isPresent() ? first.get() : null;
  }

  private List<EpisodeInfo> getEligibleEpisodeInfos(EpisodeGroupRating groupRating) throws SQLException {
    String sql = "select * " +
        "from regular_episode " +
        "where air_date between ? and ? " +
        "and series_id = ? " +
        "order by air_date";

    List<Episode> episodes = new ArrayList<>();

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, groupRating.startDate.getValue(), groupRating.endDate.getValue(), groupRating.seriesId.getValue());

    while (resultSet.next()) {
      Episode episode = new Episode();
      episode.initializeFromDBObject(resultSet);

      episodes.add(episode);
    }

    return populateInfos(episodes);
  }

  private List<EpisodeInfo> getWatchedEligibleEpisodeInfos(List<EpisodeInfo> episodeInfos, @Nullable Timestamp ratingEndDate) {
    return episodeInfos.stream()
        .filter(episodeInfo -> episodeInfo.episodeRating != null &&
            episodeInfo.episodeRating.watched.getValue() &&
            (ratingEndDate == null || watchedBeforeDate(episodeInfo.episodeRating, ratingEndDate))
        )
        .collect(Collectors.toList());
  }

  private boolean watchedBeforeDate(@NotNull EpisodeRating episodeRating, @NotNull Timestamp ratingEndDate) {
    return episodeRating.watchedDate.getValue() != null &&
        episodeRating.watchedDate.getValue().before(ratingEndDate);
  }

  private List<EpisodeInfo> populateInfos(List<Episode> episodes) throws SQLException {
    List<EpisodeInfo> infos = new ArrayList<>();
    for (Episode episode : episodes) {
      infos.add(new EpisodeInfo(episode));
    }
    return infos;
  }

  @Override
  public String getRunnerName() {
    return "Episode Group Updater";
  }

  @Override
  public @Nullable UpdateMode getUpdateMode() {
    return null;
  }

  private class EpisodeInfo {
    Episode episode;
    @Nullable EpisodeRating episodeRating;

    EpisodeInfo(Episode episode) throws SQLException {
      this.episode = episode;
      this.episodeRating = episode.getMostRecentRating(connection);
    }

  }

  @Nullable
  private BigDecimal getAvgRating(List<EpisodeInfo> episodeInfos) {
    List<BigDecimal> ratings = episodeInfos.stream()
        .map(episodeInfo -> episodeInfo.episodeRating)
        .filter(Objects::nonNull)
        .map(episodeRating -> episodeRating.ratingValue.getValue())
        .filter(Objects::nonNull)
        .collect(Collectors.toList());

    if (ratings.isEmpty()) {
      return null;
    }

    BigDecimal totalRating = ratings.stream()
        .reduce(BigDecimal::add)
        .get();
    return totalRating.divide(BigDecimal.valueOf(ratings.size()), 1, RoundingMode.HALF_EVEN);
  }

  @Nullable
  private BigDecimal getAvgFunny(List<EpisodeInfo> episodeInfos) {
    List<BigDecimal> ratings = episodeInfos.stream()
        .map(episodeInfo -> episodeInfo.episodeRating)
        .filter(Objects::nonNull)
        .map(episodeRating -> episodeRating.ratingFunny.getValue())
        .filter(Objects::nonNull)
        .collect(Collectors.toList());

    if (ratings.isEmpty()) {
      return null;
    }

    BigDecimal totalRating = ratings.stream()
        .reduce(BigDecimal::add)
        .get();
    return totalRating.divide(BigDecimal.valueOf(ratings.size()), 1, RoundingMode.HALF_EVEN);
  }

  @Nullable
  private BigDecimal getAvgCharacter(List<EpisodeInfo> episodeInfos) {
    List<BigDecimal> ratings = episodeInfos.stream()
        .map(episodeInfo -> episodeInfo.episodeRating)
        .filter(Objects::nonNull)
        .map(episodeRating -> episodeRating.ratingCharacter.getValue())
        .filter(Objects::nonNull)
        .collect(Collectors.toList());

    if (ratings.isEmpty()) {
      return null;
    }

    BigDecimal totalRating = ratings.stream()
        .reduce(BigDecimal::add)
        .get();
    return totalRating.divide(BigDecimal.valueOf(ratings.size()), 1, RoundingMode.HALF_EVEN);
  }

  @Nullable
  private BigDecimal getAvgStory(List<EpisodeInfo> episodeInfos) {
    List<BigDecimal> ratings = episodeInfos.stream()
        .map(episodeInfo -> episodeInfo.episodeRating)
        .filter(Objects::nonNull)
        .map(episodeRating -> episodeRating.ratingStory.getValue())
        .filter(Objects::nonNull)
        .collect(Collectors.toList());

    if (ratings.isEmpty()) {
      return null;
    }

    BigDecimal totalRating = ratings.stream()
        .reduce(BigDecimal::add)
        .get();
    return totalRating.divide(BigDecimal.valueOf(ratings.size()), 1, RoundingMode.HALF_EVEN);
  }

  @Nullable
  private BigDecimal getMaxRating(List<EpisodeInfo> episodeInfos) {
    Optional<BigDecimal> max = episodeInfos.stream()
        .map(episodeInfo -> episodeInfo.episodeRating)
        .filter(Objects::nonNull)
        .map(episodeRating -> episodeRating.ratingValue.getValue())
        .filter(Objects::nonNull)
        .reduce(BigDecimal::max);
    return max.isPresent() ? max.get() : null;
  }

  @Nullable
  private BigDecimal getLastRating(List<EpisodeInfo> episodeInfos) {
    Comparator<EpisodeInfo> byAirDate = Comparator.comparing(a -> a.episode.airDate.getValue());
    Comparator<EpisodeInfo> byEpisodeNumber = Comparator.comparing(a -> a.episode.episodeNumber.getValue());
    Optional<EpisodeRating> lastRating = episodeInfos.stream()
        .sorted(byAirDate.thenComparing(byEpisodeNumber).reversed())
        .map(episodeInfo -> episodeInfo.episodeRating)
        .filter(Objects::nonNull)
        .findFirst();
    return lastRating.isPresent() ? lastRating.get().ratingValue.getValue() : null;
  }

  @NotNull
  private Integer getPostUpdateEpisodes(List<EpisodeInfo> episodeInfos, Date updateDate) {
    if (updateDate == null) {
      return 0;
    }
    return toIntExact(episodeInfos.stream()
        .filter(episodeInfo -> episodeInfo.episodeRating != null &&
            episodeInfo.episodeRating.watchedDate.getValue() != null &&
            episodeInfo.episodeRating.watchedDate.getValue().after(updateDate))
        .count());
  }

  @Nullable
  private BigDecimal getSuggestedRating(EpisodeGroupRating groupRating) {
    BigDecimal average = groupRating.avgRating.getValue();
    BigDecimal max = groupRating.maxRating.getValue();
    BigDecimal last = groupRating.lastRating.getValue();

    if (average == null || max == null || last == null) {
      return null;
    }

    BigDecimal total = average.multiply(BigDecimal.valueOf(5))
        .add(max.multiply(BigDecimal.valueOf(3)))
        .add(last);

    return total.divide(BigDecimal.valueOf(9), RoundingMode.HALF_EVEN);
  }

  @NotNull
  private EpisodeGroupRating getOrCreateExistingRatingForSeriesAndYear(Series series, Integer year) throws SQLException {
    Optional<EpisodeGroupRating> existingRating = findRatingForSeriesAndYear(series, year);

    EpisodeGroupRating groupRating = new EpisodeGroupRating();
    if (existingRating.isPresent()) {
      return existingRating.get();
    } else {
      Integer previousYear = year - 1;
      Optional<EpisodeGroupRating> previousYearRating = findRatingForSeriesAndYear(series, previousYear);

      DateTime startDate = previousYearRating.isPresent() ?
          nextDay(previousYearRating.get().endDate.getValue()) :
          beginningOfYear(year);

      DateTime endDate = endOfYear(year);

      groupRating.initializeForInsert();
      groupRating.seriesId.changeValue(series.id.getValue());
      groupRating.year.changeValue(year);
      groupRating.startDate.changeValue(new Timestamp(startDate.toDate().getTime()));
      groupRating.endDate.changeValue(new Timestamp(endDate.toDate().getTime()));
    }
    return groupRating;
  }

  private DateTime nextDay(Timestamp day) {
    return new DateTime(day).plusDays(1);
  }

  @NotNull
  private DateTime endOfYear(Integer year) {
    return new DateTime(year, 12, 31, 0, 0, 0);
  }

  @NotNull
  private DateTime beginningOfYear(Integer year) {
    return new DateTime(year, 1, 1, 0, 0, 0);
  }

  private Optional<EpisodeGroupRating> findRatingForSeriesAndYear(Series series, Integer year) throws SQLException {
    String sql = "select * " +
        "from episode_group_rating " +
        "where series_id = ? " +
        "and year = ?";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, series.id.getValue(), year);

    EpisodeGroupRating groupRating = new EpisodeGroupRating();
    if (resultSet.next()) {
      groupRating.initializeFromDBObject(resultSet);
      return Optional.of(groupRating);
    } else {
      return Optional.empty();
    }
  }

}
