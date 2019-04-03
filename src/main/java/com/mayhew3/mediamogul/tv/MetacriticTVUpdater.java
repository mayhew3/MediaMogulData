package com.mayhew3.mediamogul.tv;

import com.mayhew3.mediamogul.model.tv.Season;
import com.mayhew3.mediamogul.model.tv.Series;
import com.mayhew3.mediamogul.model.tv.TVDBSeries;
import com.mayhew3.mediamogul.scheduler.UpdateRunner;
import com.mayhew3.mediamogul.tv.helper.MetacriticException;
import com.mayhew3.mediamogul.tv.helper.UpdateMode;
import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import org.joda.time.DateTime;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class MetacriticTVUpdater implements UpdateRunner {

  private SQLConnection connection;
  private UpdateMode updateMode;

  private final Map<UpdateMode, Runnable> methodMap;

  private static Logger logger = LogManager.getLogger(MetacriticTVUpdater.class);

  public MetacriticTVUpdater(SQLConnection connection, UpdateMode updateMode) {
    methodMap = new HashMap<>();
    methodMap.put(UpdateMode.FULL, this::runFullUpdate);
    methodMap.put(UpdateMode.QUICK, this::runQuickUpdate);
    methodMap.put(UpdateMode.SINGLE, this::runUpdateSingle);
    methodMap.put(UpdateMode.SANITY, this::runUpdateOnMatched);

    this.connection = connection;

    if (!methodMap.keySet().contains(updateMode)) {
      throw new IllegalArgumentException("Update mode '" + updateMode + "' is not applicable for this updater.");
    }

    this.updateMode = updateMode;
  }

  public static void main(String... args) throws URISyntaxException, SQLException {
    ArgumentChecker argumentChecker = new ArgumentChecker(args);
    UpdateMode updateMode = UpdateMode.getUpdateModeOrDefault(argumentChecker, UpdateMode.FULL);

    SQLConnection connection = PostgresConnectionFactory.createConnection(argumentChecker);
    MetacriticTVUpdater metacriticTVUpdater = new MetacriticTVUpdater(connection, updateMode);

    metacriticTVUpdater.runUpdate();
  }

  @Override
  public void runUpdate() {
    methodMap.get(updateMode).run();
  }

  void runFullUpdate() {
    String sql = "select *\n" +
        "from series\n" +
        "where tvdb_match_status = ? " +
        "and retired = ? ";

    try {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, TVDBMatchStatus.MATCH_COMPLETED, 0);
      runUpdateOnResultSet(resultSet);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void runUpdateOnMatched() {
    String sql = "select *\n" +
        "from series\n" +
        "where tvdb_match_status = ? " +
        "and metacritic IS NOT NULL " +
        "and metacritic_confirmed IS NULL " +
        "and retired = ? ";

    try {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, TVDBMatchStatus.MATCH_COMPLETED, 0);
      runUpdateOnResultSet(resultSet);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void runQuickUpdate() {
    String sql = "select *\n" +
        "from series\n" +
        "where tvdb_match_status = ? " +
        "and metacritic_new = ? " +
        "and retired = ? ";

    try {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, TVDBMatchStatus.MATCH_COMPLETED, true, 0);
      runUpdateOnResultSet(resultSet);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }


  private void runUpdateSingle() {
    String singleSeriesTitle = "Love"; // update for testing on a single series

    String sql = "select *\n" +
        "from series\n" +
        "where tvdb_match_status = ? " +
        "and title = ? " +
        "and retired = ? ";

    try {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, TVDBMatchStatus.MATCH_COMPLETED, singleSeriesTitle, 0);
      runUpdateOnResultSet(resultSet);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void runUpdateOnResultSet(ResultSet resultSet) throws SQLException {
    debug("Starting update.");

    int i = 0;

    while (resultSet.next()) {
      i++;
      Series series = new Series();
      series.initializeFromDBObject(resultSet);

      try {
        parseMetacritic(series);
        series.metacriticNew.changeValue(false);
        series.commit(connection);
      } catch (MetacriticException e) {
        logger.warn("Unable to find metacritic for: " + series.seriesTitle.getValue());
        series.metacriticNew.changeValue(false);
        series.commit(connection);
      } catch (Exception e) {
        e.printStackTrace();
        logger.error("Uncaught exception during metacritic fetch: " + series.seriesTitle.getValue());
      }

      debug(i + " processed.");
    }
    if (i == 0) {
      logger.info("No rows found to process. Finished.");
    } else {
      logger.info("Finished processing " + i + " rows.");
    }
  }

  private void parseMetacritic(Series series) throws MetacriticException, SQLException {
    String title = series.seriesTitle.getValue();
    debug("Metacritic update for: " + title);

    String matchedTitle = series.metacriticConfirmed.getValue();

    if (matchedTitle == null) {

      List<String> stringsToTry = new ArrayList<>();

      String hint = series.metacriticHint.getValue();

      if (hint != null) {
        stringsToTry.add(hint);
      }

      String formattedTitle =
          title
              .toLowerCase()
              .replaceAll(" ", "-")
              .replaceAll("'", "")
              .replaceAll("\\.", "")
              .replaceAll("\\(", "")
              .replaceAll("\\)", "");

      Optional<TVDBSeries> tvdbSeries = series.getTVDBSeries(connection);

      Date dateToCheck = tvdbSeries.isPresent() ? tvdbSeries.get().firstAired.getValue() : new Date();

      Integer year = new DateTime(dateToCheck).getYear();
      String formattedTitleWithYear = formattedTitle + "-" + year;

      stringsToTry.add(formattedTitleWithYear);
      stringsToTry.add(formattedTitle);

      matchedTitle = findMetacriticForStrings(series, stringsToTry);

      if (matchedTitle == null) {
        throw new MetacriticException("Couldn't find Metacritic page for series '" + title + "' with formatted '" + stringsToTry + "'");
      }

    } else {
      try {
        findMetacriticForString(series, matchedTitle, 1);
      } catch (Exception e) {
        throw new MetacriticException("Had trouble finding metacritic for Season 1 of series '" + title + "' even though formatted title was confirmed previously: '" + series.metacriticConfirmed.getValue() + "'");
      }
    }

    Integer seasonNumber = 1;
    boolean failed = false;

    while (!failed) {
      seasonNumber++;

      try {
        findMetacriticForString(series, matchedTitle + "/season-" + seasonNumber, seasonNumber);
      } catch (Exception e) {
        failed = true;
        debug("Finished finding seasons after Season " + (seasonNumber - 1));
      }
    }
  }

  @Nullable
  private String findMetacriticForStrings(Series series, List<String> stringsToTry) {
    for (String stringToTry : stringsToTry) {
      try {
        findMetacriticForString(series, stringToTry, 1);
        return stringToTry;
      } catch (Exception e) {
        debug("Unable to find metacritic page for string: " + stringToTry);
      }
    }
    return null;
  }

  private void findMetacriticForString(Series series, String formattedTitle, Integer seasonNumber) throws IOException, SQLException, MetacriticException {
    Document document = Jsoup.connect("http://www.metacritic.com/tv/" + formattedTitle)
        .timeout(10000)
        .userAgent("Mozilla")
        .get();

    if (seasonNumber == 1) {
      series.metacriticConfirmed.changeValue(formattedTitle);
      series.commit(connection);
    }

    if (seasonNumber > 1) {
      Elements select = document.select("[href=/tv/" + formattedTitle + "]");
      if (select.isEmpty()) {
        throw new MetacriticException("Current season doesn't exist.");
      }
    }

    Elements elements = document.select("[itemprop=ratingValue]");
    Element first = elements.first();

    if (first == null) {
      throw new MetacriticException("Page found, but no element found with 'ratingValue' id.");
    }

    Node metacriticValue = first.childNodes().get(0);

    Integer metaCritic = Integer.valueOf(metacriticValue.toString());

    String sql = "SELECT * " +
        "FROM season " +
        "WHERE series_id = ? " +
        "AND season_number = ?";
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, series.id.getValue(), seasonNumber);

    Season season = new Season();

    if (resultSet.next()) {
      season.initializeFromDBObject(resultSet);
    } else {
      season.initializeForInsert();
      season.dateModified.changeValue(new Date());
    }

    debug("Updating Season " + seasonNumber + " (" + metaCritic + ")");

    season.metacritic.changeValue(metaCritic);
    season.seasonNumber.changeValue(seasonNumber);
    season.seriesId.changeValue(series.id.getValue());

    if (season.hasChanged()) {
      season.dateModified.changeValue(new Date());
    }

    season.commit(connection);

    series.metacritic.changeValue(metaCritic);
    series.commit(connection);
  }

  private void debug(Object message) {
    logger.debug(message);
  }

  @Override
  public String getRunnerName() {
    return "Metacritic TV Runner";
  }

  @Override
  public @Nullable UpdateMode getUpdateMode() {
    return updateMode;
  }

}
