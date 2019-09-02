package com.mayhew3.mediamogul.tv.blog;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.mayhew3.mediamogul.model.tv.Episode;
import com.mayhew3.mediamogul.model.tv.EpisodeGroupRating;
import com.mayhew3.mediamogul.model.tv.EpisodeRating;
import com.mayhew3.mediamogul.model.tv.Series;
import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class BlogRankingsCreator {

  private SQLConnection connection;
  private BlogTemplatePrinter standardTemplate;
  private BlogTemplatePrinter topTemplate;
  private BlogTemplatePrinter toppestTemplate;
  private String outputPath;

  private List<Integer> postBoundaries = Lists.newArrayList(43, 20, 10, 0);

  private static Logger logger = LogManager.getLogger(BlogRankingsCreator.class);

  private BlogRankingsCreator(SQLConnection connection, String templatePath, String outputPath) throws IOException {
    this.connection = connection;

    this.standardTemplate = createTemplate(templatePath + "/yearly_template.html");
    this.topTemplate = createTemplate(templatePath + "/top20_template.html");
    this.toppestTemplate = createTemplate(templatePath + "/top1_template.html");

    this.outputPath = outputPath;
  }

  public static void main(String... args) throws URISyntaxException, SQLException, IOException {
    ArgumentChecker argumentChecker = new ArgumentChecker(args);
    argumentChecker.addExpectedOption("template", true, "Template file name");
    argumentChecker.addExpectedOption("blog", true, "Blog file name");

    Optional<String> templateFilePath = argumentChecker.getOptionalIdentifier("template");
    if (templateFilePath.isEmpty()) {
      throw new IllegalStateException("No 'template' argument provided.");
    }

    Optional<String> blogOutputFilePath = argumentChecker.getOptionalIdentifier("blog");
    if (blogOutputFilePath.isEmpty()) {
      throw new IllegalStateException("No 'blog' argument provided.");
    }

    SQLConnection connection = PostgresConnectionFactory.createConnection(argumentChecker);

    BlogRankingsCreator blogRankingsCreator = new BlogRankingsCreator(connection, templateFilePath.get(), blogOutputFilePath.get());
    blogRankingsCreator.execute();
  }

  private BlogTemplatePrinter createTemplate(String fileName) throws IOException {
    Path templateFile = Paths.get(fileName);
    String template = new String(Files.readAllBytes(templateFile));
    return new BlogTemplatePrinter(template);
  }

  private void execute() throws IOException, SQLException {
    Map<Integer, String> combinedExports = fetchSeriesAndCombineWithTemplate();

    for (Integer boundary : postBoundaries) {
      File outputFile = new File(outputPath + "/blog_output" + (boundary + 1) + ".html");
      FileOutputStream outputStream = new FileOutputStream(outputFile, false);

      String combinedExport = combinedExports.get(boundary);

      outputStream.write(combinedExport.getBytes());
      outputStream.close();
    }

  }



  private Map<Integer, String> fetchSeriesAndCombineWithTemplate() throws SQLException {

    Map<Integer, StringBuilder> exportBuilders = Maps.newHashMap();

    for (Integer postBoundary : postBoundaries) {
      exportBuilders.put(postBoundary, new StringBuilder());
    }

    String reusableJoins = "FROM episode_group_rating " +
        "WHERE year = ? " +
        "AND retired = ? " +
        "AND aired > ? " +
        "AND aired = watched " +
        "AND rating IS NOT NULL ";

    logger.info("Getting series count...");

    Integer totalShows = getSeriesCount(reusableJoins);

    logger.info("Done. Processing series...");

    String fullSql = "SELECT * " +
        reusableJoins +
        "ORDER BY coalesce(rating, suggested_rating)  ASC ";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(fullSql, 2017, 0, 0);

    Integer currentRanking = totalShows;

    while (resultSet.next()) {
      StringBuilder export = exportBuilders.get(getNextBoundary(currentRanking));

      EpisodeGroupRating episodeGroupRating = new EpisodeGroupRating();
      episodeGroupRating.initializeFromDBObject(resultSet);

      BlogTemplatePrinter templateToUse = currentRanking > 20 ? standardTemplate :
          currentRanking == 1 ? toppestTemplate : topTemplate;

      export.append(getExportForSeries(templateToUse, episodeGroupRating, currentRanking));
      export.append("<br>");

      debug("Export created and added to big string.");

      currentRanking--;
    }

    Map<Integer, String> allExports = Maps.newHashMap();

    for (Integer boundary : exportBuilders.keySet()) {
      allExports.put(boundary, exportBuilders.get(boundary).toString());
    }

    return allExports;
  }

  private Integer getNextBoundary(Integer currentRanking) {
    Optional<Integer> remainingBoundaries = postBoundaries.stream()
        .filter(boundary -> boundary < currentRanking)
        .findFirst();

    if (remainingBoundaries.isPresent()) {
      return remainingBoundaries.get();
    } else {
      throw new IllegalStateException("Current ranking " + currentRanking + " with no boundaries less than it.");
    }
  }

  @NotNull
  private Integer getSeriesCount(String reusableJoins) throws SQLException {
    String countSql = "SELECT COUNT(1) as series_count " +
        reusableJoins;

    ResultSet resultSet1 = connection.prepareAndExecuteStatementFetch(countSql, 2017, 0, 0);

    Integer totalShows = 0;
    if (resultSet1.next()) {
      totalShows = resultSet1.getInt("series_count");
    }
    return totalShows;
  }

  @SuppressWarnings("ConstantConditions")
  private String getExportForSeries(BlogTemplatePrinter blogTemplatePrinter, EpisodeGroupRating episodeGroupRating, Integer currentRanking) throws SQLException {
    blogTemplatePrinter.clearMappings();

    Series series = getSeries(episodeGroupRating);

    debug(currentRanking + ": " + series.seriesTitle.getValue());

    tryToSavePosterLocally(series);

    BigDecimal effectiveRating = episodeGroupRating.rating.getValue() == null ?
        episodeGroupRating.suggestedRating.getValue() :
        episodeGroupRating.rating.getValue();

    debug("Getting episode infos...");

    List<EpisodeInfo> episodeInfos = getEligibleEpisodeInfos(episodeGroupRating);

    debug("Gotten. Adding mappings...");

    EpisodeInfo bestEpisode = getBestEpisode(episodeGroupRating, episodeInfos);

    BigDecimal bestEpisodeRating = bestEpisode.episodeRating.ratingValue.getValue();

    blogTemplatePrinter.addMapping("POSTER_FILENAME", generatePosterName(series, currentRanking));
    blogTemplatePrinter.addMapping("RANKING_VALUE", Integer.toString(currentRanking));
    blogTemplatePrinter.addMapping("RATING_COLOR", getHSLAMethod(effectiveRating));
    blogTemplatePrinter.addMapping("RATING_VALUE", effectiveRating.toString());
    blogTemplatePrinter.addMapping("SERIES_NAME", series.seriesTitle.getValue());
    blogTemplatePrinter.addMapping("SEASONS_TEXT", getSeasonString(getSeasons(episodeInfos)));
    blogTemplatePrinter.addMapping("EPISODE_COUNT", Integer.toString(episodeGroupRating.aired.getValue()));
    blogTemplatePrinter.addMapping("FEATURED_RATING_COLOR", getHSLAMethod(bestEpisodeRating));
    blogTemplatePrinter.addMapping("FEATURED_RATING_VALUE", bestEpisodeRating.toString());
    blogTemplatePrinter.addMapping("FEATURED_EPISODE_NUMBER", bestEpisode.episode.getSeason() + "x" + bestEpisode.episode.episodeNumber.getValue());
    blogTemplatePrinter.addMapping("FEATURED_EPISODE_TITLE", bestEpisode.episode.title.getValue());
    blogTemplatePrinter.addMapping("REVIEW_TEXT", episodeGroupRating.review.getValue());

    debug("Mappings added. Creating export...");

    return blogTemplatePrinter.createCombinedExport();
  }

  private String generatePosterName(Series series, Integer currentRanking) {
    if (currentRanking > 20) {
      String fullPosterName = series.poster.getValue();
      return fullPosterName.replace("posters/", "");
    } else {
      String fullSeriesName = series.seriesTitle.getValue();
      fullSeriesName = fullSeriesName.replace(" ", "_");
      fullSeriesName = fullSeriesName.replace("'", "");
      fullSeriesName = fullSeriesName.replace(".", "");
      return fullSeriesName.toLowerCase();
    }
  }

  private void tryToSavePosterLocally(Series series) {
    String seriesPosterFileName = series.poster.getValue();
    if (seriesPosterFileName != null) {
      try {
        InputStream inputStream = new URL("https://www.thetvdb.com/banners/" + seriesPosterFileName).openStream();
        String fullFilePath = outputPath + "/" + seriesPosterFileName;
        if (!new File(fullFilePath).exists()) {
          Files.copy(inputStream, Paths.get(fullFilePath));
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

  }

  private String getSeasonString(List<Integer> seasonList) {
    if (seasonList.size() == 1) {
      return "Season " + seasonList.get(0);
    } else {
      return "Seasons " + Joiner.on('/').join(seasonList);
    }
  }

  private List<Integer> getSeasons(List<EpisodeInfo> episodeInfos) {
    Set<Integer> seasonSet = episodeInfos.stream()
        .map(episodeInfo -> episodeInfo.episode.getSeason())
        .collect(Collectors.toSet());

    return Lists.newArrayList(seasonSet).stream()
        .sorted()
        .collect(Collectors.toList());
  }

  @NotNull
  private BlogRankingsCreator.EpisodeInfo getBestEpisode(EpisodeGroupRating episodeGroupRating, List<EpisodeInfo> episodeInfos) {
    Optional<EpisodeInfo> first = episodeInfos.stream()
        .filter(episodeInfo -> episodeInfo.episodeRating != null &&
            episodeGroupRating.maxRating.getValue().equals(episodeInfo.episodeRating.ratingValue.getValue()))
        .findFirst();

    if (!first.isPresent()) {
      throw new IllegalStateException("No episode found with max rating!");
    }

    return first.get();
  }

  private List<EpisodeInfo> getEligibleEpisodeInfos(EpisodeGroupRating groupRating) throws SQLException {
    List<Episode> episodes = getEpisodes(groupRating);
    List<EpisodeRating> episodeRatings = getEpisodeRatings(groupRating);

    return populateInfos(episodes, episodeRatings);
  }

  @NotNull
  private List<Episode> getEpisodes(EpisodeGroupRating groupRating) throws SQLException {
    String sql = "select *\n" +
        "from episode\n" +
        "where air_date between ? and ?\n" +
        "and series_id = ?\n" +
        "and season <> ? \n" +
        "and retired = ?\n" +
        "order by air_date";

    List<Episode> episodes = new ArrayList<>();

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, groupRating.startDate.getValue(), groupRating.endDate.getValue(), groupRating.seriesId.getValue(), 0, 0);

    while (resultSet.next()) {
      Episode episode = new Episode();
      episode.initializeFromDBObject(resultSet);

      episodes.add(episode);
    }
    return episodes;
  }

  private List<EpisodeRating> getEpisodeRatings(EpisodeGroupRating groupRating) throws SQLException {
    String sql = "select er.* " +
        "from episode e " +
        "inner join episode_rating er " +
        "  on er.episode_id = e.id " +
        "where e.air_date between ? and ? " +
        "and e.series_id = ? " +
        "and e.season <> ? " +
        "and e.retired = ? " +
        "order by e.air_date";

    List<EpisodeRating> episodeRatings = new ArrayList<>();

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, groupRating.startDate.getValue(), groupRating.endDate.getValue(), groupRating.seriesId.getValue(), 0, 0);

    while (resultSet.next()) {
      EpisodeRating episodeRating = new EpisodeRating();
      episodeRating.initializeFromDBObject(resultSet);

      episodeRatings.add(episodeRating);
    }
    return episodeRatings;
  }

  private List<EpisodeInfo> populateInfos(List<Episode> episodes, List<EpisodeRating> episodeRatings) throws SQLException {
    List<EpisodeInfo> infos = new ArrayList<>();
    for (Episode episode : episodes) {
      EpisodeRating mostRecentRating = getMostRecentRating(episode, episodeRatings);
      infos.add(new EpisodeInfo(episode, mostRecentRating));
    }
    return infos;
  }

  private class EpisodeInfo {
    Episode episode;
    @Nullable EpisodeRating episodeRating;

    EpisodeInfo(Episode episode, @Nullable EpisodeRating episodeRating) {
      this.episode = episode;
      this.episodeRating = episodeRating;
    }

  }

  @Nullable
  private EpisodeRating getMostRecentRating(Episode episode, List<EpisodeRating> episodeRatings) {
    Optional<EpisodeRating> mostRecent = episodeRatings.stream()
        .filter(episodeRating -> episodeRating.episodeId.getValue().equals(episode.id.getValue()))
        .max(Comparator.comparing(rating -> rating.watchedDate.getValue()));

    return mostRecent
        .orElse(null);
  }


  private Series getSeries(EpisodeGroupRating episodeGroupRating) throws SQLException {
    String sql = "SELECT * " +
        "FROM series " +
        "WHERE id = ? ";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, episodeGroupRating.seriesId.getValue());
    if (resultSet.next()) {
      Series series = new Series();
      series.initializeFromDBObject(resultSet);

      return series;
    } else {
      throw new IllegalStateException("No series found with id: " + episodeGroupRating.seriesId.getValue() + ", linked to EpisodeGroupRating id: " + episodeGroupRating.id.getValue());
    }
  }

  private String getHSLAMethod(BigDecimal value) {
    BigDecimal hue = getHue(value);
    String saturation = (value == null) ? "0%" : "50%";
    return "hsla(" + hue + ", " + saturation + ", 42%, 1)";
  }

  private BigDecimal getHue(BigDecimal value) {
    if (value == null) {
      return BigDecimal.ZERO;
    }

    BigDecimal fifty = BigDecimal.valueOf(50);
    BigDecimal half = BigDecimal.valueOf(.5);

    // matches javascript code from seriesDetailController
    return
        (value.compareTo(fifty) <= 0) ?
        value.multiply(half) :
        (fifty.multiply(half).add((value.subtract(fifty).multiply(BigDecimal.valueOf(4.5)))));
  }


  private void debug(Object message) {
    logger.debug(message);
  }

}
