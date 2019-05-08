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

  MetacriticTVUpdater(Series series, SQLConnection connection) {
    this.series = series;
    this.connection = connection;
  }

  void parseMetacritic() throws MetacriticException, SQLException {
    String title = series.seriesTitle.getValue();
    logger.debug("Metacritic update for: " + title);

    String matchedTitle = series.metacriticConfirmed.getValue();

    List<Season> seasons = getSeasons(series);
    Season firstSeason = seasons.get(0);

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

      int year = new DateTime(dateToCheck).getYear();
      String formattedTitleWithYear = formattedTitle + "-" + year;

      stringsToTry.add(formattedTitleWithYear);
      stringsToTry.add(formattedTitle);

      matchedTitle = findMetacriticForStrings(stringsToTry, firstSeason);

      if (matchedTitle == null) {
        throw new MetacriticException("Couldn't find Metacritic page for series '" + title + "' with formatted '" + stringsToTry + "'");
      }

    } else {
      try {
        findMetacriticForString(matchedTitle, firstSeason);
      } catch (IOException e) {
        throw new MetacriticException("Had trouble finding metacritic for Season 1 of series '" + title + "' even though formatted title was confirmed previously: '" + series.metacriticConfirmed.getValue() + "'");
      }
    }

    for (Season season : seasons) {
      Integer seasonNumber = season.seasonNumber.getValue();
      try {
        findMetacriticForString(matchedTitle + "/season-" + seasonNumber, season);
      } catch (IOException e) {
        logger.debug("No metacritic page found for Season " + seasonNumber);
      } catch (Exception e) {
        logger.debug("Found metacritic page but failed to get score: " + e.getLocalizedMessage());
      }
    }
  }

  private List<Season> getSeasons(Series series) throws SQLException {
    String sql = "SELECT * " +
        "FROM season " +
        "WHERE series_id = ? " +
        "AND retired = ? " +
        "AND season_number <> ? " +
        "ORDER BY season_number ";
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, series.id.getValue(), 0, 0);

    List<Season> seasons = new ArrayList<>();
    while(resultSet.next()) {
      Season season = new Season();
      season.initializeFromDBObject(resultSet);
      seasons.add(season);
    }
    return seasons;
  }

  @Nullable
  private String findMetacriticForStrings(List<String> stringsToTry, Season firstSeason) {
    for (String stringToTry : stringsToTry) {
      try {
        findMetacriticForString(stringToTry, firstSeason);
        return stringToTry;
      } catch (Exception e) {
        logger.debug("Unable to find metacritic page for string: " + stringToTry);
      }
    }
    return null;
  }

  private void findMetacriticForString(String formattedTitle, Season season) throws IOException, SQLException, MetacriticException {
    Document document = Jsoup.connect("http://www.metacritic.com/tv/" + formattedTitle)
        .timeout(10000)
        .userAgent("Mozilla")
        .get();

    Integer seasonNumber = season.seasonNumber.getValue();
    if (seasonNumber == 1) {
      series.metacriticConfirmed.changeValue(formattedTitle);
      series.commit(connection);
    }

    Element first;
    try {
      Element primaryBabyItemDiv = document.select(".primary_baby_item").first();
      first = primaryBabyItemDiv.select(".metascore_w").first();
    } catch (Exception e) {
      throw new MetacriticException("Page found, but no element found with 'primary_baby_item' id.");
    }

    Node metacriticValue = first.childNodes().get(0);

    Integer metaCritic;
    try {
      metaCritic = Integer.valueOf(metacriticValue.toString());
    } catch (NumberFormatException e) {
      throw new MetacriticException("Found metacritic score element, but it had non-numeric value.");
    }

    logger.debug("Updating Season " + seasonNumber + " (" + metaCritic + ")");

    season.metacritic.changeValue(metaCritic);
    season.seriesId.changeValue(series.id.getValue());

    if (season.hasChanged()) {
      season.dateModified.changeValue(new Date());
    }

    season.commit(connection);

    series.metacritic.changeValue(metaCritic);
    series.commit(connection);
  }

}
