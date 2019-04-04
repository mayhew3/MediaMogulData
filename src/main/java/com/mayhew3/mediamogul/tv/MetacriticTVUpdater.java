package com.mayhew3.mediamogul.tv;

import com.mayhew3.mediamogul.model.tv.Season;
import com.mayhew3.mediamogul.model.tv.Series;
import com.mayhew3.mediamogul.model.tv.TVDBSeries;
import com.mayhew3.mediamogul.tv.helper.MetacriticException;
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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;

public class MetacriticTVUpdater {
  private Series series;

  private SQLConnection connection;

  private static Logger logger = LogManager.getLogger(MetacriticTVUpdater.class);

  public MetacriticTVUpdater(Series series, SQLConnection connection) {
    this.series = series;
    this.connection = connection;
  }

  public void parseMetacritic() throws MetacriticException, SQLException {
    String title = series.seriesTitle.getValue();
    logger.debug("Metacritic update for: " + title);

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

      matchedTitle = findMetacriticForStrings(stringsToTry);

      if (matchedTitle == null) {
        throw new MetacriticException("Couldn't find Metacritic page for series '" + title + "' with formatted '" + stringsToTry + "'");
      }

    } else {
      try {
        findMetacriticForString(matchedTitle, 1);
      } catch (Exception e) {
        throw new MetacriticException("Had trouble finding metacritic for Season 1 of series '" + title + "' even though formatted title was confirmed previously: '" + series.metacriticConfirmed.getValue() + "'");
      }
    }

    Integer seasonNumber = 1;
    boolean failed = false;

    while (!failed) {
      seasonNumber++;

      try {
        findMetacriticForString(matchedTitle + "/season-" + seasonNumber, seasonNumber);
      } catch (Exception e) {
        failed = true;
        logger.debug("Finished finding seasons after Season " + (seasonNumber - 1));
      }
    }
  }

  @Nullable
  private String findMetacriticForStrings(List<String> stringsToTry) {
    for (String stringToTry : stringsToTry) {
      try {
        findMetacriticForString(stringToTry, 1);
        return stringToTry;
      } catch (Exception e) {
        logger.debug("Unable to find metacritic page for string: " + stringToTry);
      }
    }
    return null;
  }

  private void findMetacriticForString(String formattedTitle, Integer seasonNumber) throws IOException, SQLException, MetacriticException {
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

    logger.debug("Updating Season " + seasonNumber + " (" + metaCritic + ")");

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

}
