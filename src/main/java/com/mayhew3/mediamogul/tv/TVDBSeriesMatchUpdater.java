package com.mayhew3.mediamogul.tv;

import com.mashape.unirest.http.exceptions.UnirestException;
import com.mayhew3.mediamogul.model.tv.PossibleSeriesMatch;
import com.mayhew3.mediamogul.model.tv.Series;
import com.mayhew3.mediamogul.tv.exception.ShowFailedException;
import com.mayhew3.mediamogul.tv.provider.TVDBJWTProvider;
import com.mayhew3.mediamogul.xml.BadlyFormattedXMLException;
import com.mayhew3.mediamogul.xml.JSONReader;
import com.mayhew3.postgresobject.db.SQLConnection;
import org.apache.http.auth.AuthenticationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONObject;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;

public class TVDBSeriesMatchUpdater {

  private Series series;

  private SQLConnection connection;
  private TVDBJWTProvider tvdbDataProvider;
  private JSONReader jsonReader;

  private static Logger logger = LogManager.getLogger(TVDBSeriesMatchUpdater.class);

  public TVDBSeriesMatchUpdater(SQLConnection connection,
                                @NotNull Series series,
                                TVDBJWTProvider tvdbWebProvider, JSONReader jsonReader) {
    this.series = series;
    this.connection = connection;
    this.tvdbDataProvider = tvdbWebProvider;
    this.jsonReader = jsonReader;
  }

  void updateSeries() throws SQLException, ShowFailedException, UnirestException, BadlyFormattedXMLException, AuthenticationException {
    try {
      findTVDBMatch(series);
    } catch (IOException | SAXException | AuthenticationException e) {
      e.printStackTrace();
      // todo: add error log
      throw new ShowFailedException("Error downloading XML from TVDB.");
    }
  }

  private void findTVDBMatch(Series series) throws SQLException, IOException, SAXException, UnirestException, ShowFailedException, AuthenticationException {
    String seriesTitle = series.seriesTitle.getValue();
    String tivoId = series.tivoSeriesV2ExtId.getValue();
    String tvdbHint = series.tvdbHint.getValue();

    String titleToCheck = (tvdbHint == null || "".equals(tvdbHint)) ? seriesTitle : tvdbHint;

    if (titleToCheck == null) {
      throw new RuntimeException("Title to check is null. TiVoSeriesId: " + tivoId);
    }

    String formattedTitle = titleToCheck
        .toLowerCase()
        .replaceAll(" ", "_");

    List<PossibleSeriesMatch> orderedMatches = getOrderedMatches(seriesTitle, formattedTitle);

    if (orderedMatches.isEmpty()) {
      if (tvdbHint != null) {
        series.tvdbMatchStatus.changeValue("Needs Better Hint");
      } else {
        series.tvdbMatchStatus.changeValue("Needs Hint");
      }
    } else {
      for (PossibleSeriesMatch possibleSeriesMatch : orderedMatches) {
        series.addPossibleSeriesMatch(connection, possibleSeriesMatch);
      }

      PossibleSeriesMatch bestMatch = orderedMatches.get(0);

      Optional<Series> differentSeriesWithSameTVDBID = findDifferentSeriesWithSameTVDBID(series.id.getValue(), bestMatch.tvdbSeriesExtId.getValue());
      series.tvdbMatchId.changeValue(bestMatch.tvdbSeriesExtId.getValue());
      if (differentSeriesWithSameTVDBID.isPresent()) {
        logger.info("Duplicate series found with TVDB ID " + bestMatch.tvdbSeriesExtId.getValue() + ": " + differentSeriesWithSameTVDBID.get());
        series.tvdbMatchStatus.changeValue("Duplicate");
      } else {
        series.tvdbMatchStatus.changeValue(TVDBMatchStatus.NEEDS_CONFIRMATION);
      }
    }

    series.commit(connection);
  }

  private Optional<Series> findDifferentSeriesWithSameTVDBID(Integer seriesId, Integer tvdbSeriesExtId) throws SQLException {
    String sql = "SELECT * " +
        "FROM series " +
        "WHERE id <> ? " +
        "AND tvdb_series_ext_id = ? " +
        "and retired = ? ";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, seriesId, tvdbSeriesExtId, 0);

    if (resultSet.next()) {
      Series series = new Series();
      series.initializeFromDBObject(resultSet);
      return Optional.of(series);
    } else {
      return Optional.empty();
    }
  }

  private List<PossibleSeriesMatch> getOrderedMatches(String seriesTitle, String formattedTitle) throws AuthenticationException, UnirestException, SQLException {
    List<PossibleSeriesMatch> possibleMatches = new ArrayList<>();

    Optional<PossibleSeriesMatch> exactMatchWithYear = findExactMatchWithYear(seriesTitle, formattedTitle);
    exactMatchWithYear.ifPresent(possibleMatches::add);

    JSONArray otherMatches = findMatchesFor(seriesTitle, formattedTitle);

    Integer maximumCapacity = exactMatchWithYear.isPresent() ? 4 : 5;
    Integer possibleSeries = Math.min(maximumCapacity, otherMatches.length());
    for (int i = 0; i < possibleSeries; i++) {
      JSONObject seriesNode = otherMatches.getJSONObject(i);
      possibleMatches.add(createPossibleSeriesMatchFromObject(seriesNode));
    }

    return possibleMatches;
  }

  private Optional<String> getTopPosterFor(Integer tvdbID) throws UnirestException, AuthenticationException {

    JSONObject imageData = tvdbDataProvider.getPosterData(tvdbID);
    JSONArray images = jsonReader.getArrayWithKey(imageData, "data");

    if (images.length() == 0) {
      return Optional.empty();
    } else {
      JSONObject firstImage = images.getJSONObject(0);

      return Optional.of(jsonReader.getStringWithKey(firstImage, "fileName"));
    }
  }

  private Optional<PossibleSeriesMatch> findExactMatchWithYear(String seriesTitle, String formattedTitle) throws AuthenticationException, UnirestException, SQLException {
    Integer year = new DateTime(new Date()).getYear();
    String formattedTitleWithYear = formattedTitle + "_(" + year + ")";

    JSONArray seriesNodes = findMatchesFor(seriesTitle, formattedTitleWithYear);

    if (seriesNodes.length() == 0) {
      return Optional.empty();
    }

    JSONObject firstSeries = seriesNodes.getJSONObject(0);

    return Optional.of(createPossibleSeriesMatchFromObject(firstSeries));
  }

  @NotNull
  private PossibleSeriesMatch createPossibleSeriesMatchFromObject(JSONObject firstSeries) throws UnirestException, AuthenticationException, SQLException {
    PossibleSeriesMatch possibleSeriesMatch = new PossibleSeriesMatch();
    possibleSeriesMatch.initializeForInsert();

    String tvdbSeriesName = firstSeries.getString("seriesName");
    Integer tvdbSeriesId = firstSeries.getInt("id");

    possibleSeriesMatch.tvdbSeriesTitle.changeValue(tvdbSeriesName);
    possibleSeriesMatch.tvdbSeriesExtId.changeValue(tvdbSeriesId);

    Optional<String> topPoster = getTopPosterFor(tvdbSeriesId);
    topPoster.ifPresent(s -> possibleSeriesMatch.poster.changeValue(s));

    Optional<Series> existing = Series.findSeriesFromTVDBExtID(tvdbSeriesId, connection);
    possibleSeriesMatch.alreadyExists.changeValue(existing.isPresent());

    return possibleSeriesMatch;
  }

  @NotNull
  private JSONArray findMatchesFor(String seriesTitle, String formattedTitle) throws UnirestException, AuthenticationException {
    debug("Update for: " + seriesTitle + ", formatted as '" + formattedTitle + "'");

    JSONObject seriesMatches = tvdbDataProvider.findSeriesMatches(formattedTitle);
    return seriesMatches.has("data") ? seriesMatches.getJSONArray("data") : new JSONArray();
  }

  private void debug(Object message) {
    logger.debug(message);
  }


}
