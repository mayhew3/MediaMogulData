package com.mayhew3.mediamogul.tv;

import com.mayhew3.mediamogul.MetacriticUpdater;
import com.mayhew3.mediamogul.exception.SingleFailedException;
import com.mayhew3.mediamogul.model.tv.Season;
import com.mayhew3.mediamogul.model.tv.Series;
import com.mayhew3.mediamogul.model.tv.TVDBSeries;
import com.mayhew3.mediamogul.tv.helper.MetacriticException;
import com.mayhew3.postgresobject.db.SQLConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import org.joda.time.DateTime;
import org.jsoup.nodes.Document;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;

public class MetacriticTVUpdater extends MetacriticUpdater {
  private final Series series;

  private static final Logger logger = LogManager.getLogger(MetacriticTVUpdater.class);

  MetacriticTVUpdater(Series series, SQLConnection connection) {
    super(connection);
    this.series = series;
  }

  void parseMetacritic() throws MetacriticException, SQLException {
    String title = series.seriesTitle.getValue();
    logger.debug("Metacritic update for: " + title);

    String matchedTitle = series.metacriticConfirmed.getValue();

    List<Season> seasons = getSeasons(series);
    if (seasons.size() > 0) {
      Season firstSeason = seasons.get(0);

      if (matchedTitle == null) {

        List<String> stringsToTry = new ArrayList<>();

        String hint = series.metacriticHint.getValue();

        if (hint != null) {
          stringsToTry.add(hint);
        }

        String formattedTitle = formatTitle(title, hint);

        Optional<TVDBSeries> tvdbSeries = series.getTVDBSeries(connection);

        Date dateToCheck = tvdbSeries.isPresent() ? tvdbSeries.get().firstAired.getValue() : new Date();

        int year = new DateTime(dateToCheck).getYear();
        String formattedTitleWithYear = formattedTitle + "-" + year;

        stringsToTry.add(formattedTitleWithYear);
        stringsToTry.add(formattedTitle);

        matchedTitle = findMetacriticForStrings(stringsToTry, firstSeason);

        if (matchedTitle == null) {
          markFailed();
          throw new MetacriticException("Couldn't find Metacritic page for series '" + title + "' with formatted '" + stringsToTry + "'");
        }
      } else {
        try {
          findMetacriticForString(matchedTitle, firstSeason);
        } catch (SingleFailedException e) {
          markFailed();
          throw new MetacriticException("Had trouble finding metacritic page for Season 1 of series '" + title + "' even though formatted title was confirmed previously: '" + series.metacriticConfirmed.getValue() + "'");
        }
      }

      for (Season season : seasons) {
        Integer seasonNumber = season.seasonNumber.getValue();
        if (seasonNumber > 1) {
          try {
            findMetacriticForString(matchedTitle + "/season-" + seasonNumber, season);
          } catch (SingleFailedException e) {
            logger.debug("No metacritic page found for Season " + seasonNumber);
          } catch (Exception e) {
            logger.debug("Found metacritic page for Season " + seasonNumber + " but failed to get score: " + e.getLocalizedMessage());
          }
        }
      }

      if (series.metacritic.getValue() == null) {
        markFailed();
      } else {
        markSucceeded();
      }


    } else {
      markFailed();
    }

  }

  private void markSucceeded() throws SQLException {
    series.metacritic_success.changeValue(new Date());
    series.metacritic_failed.changeValue(null);
    series.commit(connection);
  }

  private void markFailed() throws SQLException {
    series.metacritic_failed.changeValue(new Date());
    series.metacritic_success.changeValue(null);
    series.commit(connection);
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

  private void findMetacriticForString(String formattedTitle, Season season) throws SQLException, SingleFailedException {
    String prefix = "tv/" + formattedTitle;
    Document document = getDocument(prefix, series.seriesTitle.getValue());

    Integer seasonNumber = season.seasonNumber.getValue();

    logger.debug("Found page for Season " + seasonNumber);

    if (seasonNumber == 1) {
      series.metacriticConfirmed.changeValue(formattedTitle);
      series.commit(connection);
    }

    Integer metaCritic = getMetacriticFromDocument(document);

    logger.debug("Found Metacritic value for Season " + seasonNumber + " (" + metaCritic + ")");

    season.metacritic.changeValue(metaCritic);
    season.seriesId.changeValue(series.id.getValue());

    if (season.hasChanged()) {
      season.dateModified.changeValue(new Date());
    }

    season.commit(connection);

    if (series.metacritic_season.getValue() == null ||
        seasonNumber >= series.metacritic_season.getValue()) {
      series.metacritic.changeValue(metaCritic);
      series.metacritic_season.changeValue(seasonNumber);
    }
    series.commit(connection);
  }

}
