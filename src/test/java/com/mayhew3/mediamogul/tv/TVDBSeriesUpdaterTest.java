package com.mayhew3.mediamogul.tv;

import com.mashape.unirest.http.exceptions.UnirestException;
import com.mayhew3.mediamogul.DatabaseTest;
import com.mayhew3.mediamogul.exception.MissingEnvException;
import com.mayhew3.mediamogul.model.tv.Episode;
import com.mayhew3.mediamogul.model.tv.Series;
import com.mayhew3.mediamogul.model.tv.TVDBEpisode;
import com.mayhew3.mediamogul.model.tv.TVDBSeries;
import com.mayhew3.mediamogul.tv.exception.ShowFailedException;
import com.mayhew3.mediamogul.tv.provider.TVDBJWTProvider;
import com.mayhew3.mediamogul.tv.provider.TVDBLocalJSONProvider;
import com.mayhew3.mediamogul.xml.BadlyFormattedXMLException;
import com.mayhew3.mediamogul.xml.JSONReaderImpl;
import org.apache.http.auth.AuthenticationException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.joda.time.LocalDate;
import org.junit.Test;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.junit.Assert.fail;

@SuppressWarnings({"SameParameterValue", "OptionalGetWithoutIsPresent"})
public class TVDBSeriesUpdaterTest extends DatabaseTest {

  private final String SCHUMER_EPISODE_NAME1 = "The World's Most Interesting Woman in the World";
  private final String SCHUMER_EPISODE_NAME2 = "Welcome to the Gun Show";
  private final String SCHUMER_EPISODE_NAME3 = "Brave";
  private String SCHUMER_SERIES_NAME = "Inside Amy Schumer";
  private int SCHUMER_SERIES_ID = 265374;
  private int SCHUMER_EPISODE_ID1 = 5578415;
  private int SCHUMER_EPISODE_ID2 = 5580497;
  private int SCHUMER_EPISODE_ID3 = 5552985;

  private TVDBJWTProvider tvdbjwtProvider;

  @Override
  public void setUp() throws URISyntaxException, SQLException, MissingEnvException {
    super.setUp();
    tvdbjwtProvider = new TVDBLocalJSONProvider("src\\test\\resources\\TVDBTest\\");
  }

  @Test
  public void testIDChangedForTVDBEpisode() throws SQLException, ShowFailedException, BadlyFormattedXMLException, UnirestException, AuthenticationException {
    createSeries(SCHUMER_SERIES_NAME, SCHUMER_SERIES_ID);

    Series series = findSeriesWithTitle(SCHUMER_SERIES_NAME);

    // fake ID - use so that XML will find a different ID and change it and not add a new episode with same episode number.
    Integer originalID = 5;

    addEpisode(series, 4, 1, SCHUMER_EPISODE_NAME1, SCHUMER_EPISODE_ID1);
    addEpisode(series, 4, 2, SCHUMER_EPISODE_NAME2, originalID);
    addEpisode(series, 4, 3, SCHUMER_EPISODE_NAME3, SCHUMER_EPISODE_ID3);

    TVDBSeriesUpdater tvdbSeriesUpdater = new TVDBSeriesUpdater(connection, series, tvdbjwtProvider, new JSONReaderImpl());
    tvdbSeriesUpdater.updateSeries();

    TVDBEpisode retiredEpisode = findTVDBEpisodeWithTVDBID(originalID);
    assertThat(retiredEpisode)
        .isNull();

    TVDBEpisode updatedEpisode = findTVDBEpisodeWithTVDBID(SCHUMER_EPISODE_ID2);
    assertThat(updatedEpisode)
        .isNotNull();
    //noinspection ConstantConditions
    assertThat(updatedEpisode.retired.getValue())
        .isEqualTo(0);
    assertThat(updatedEpisode.seasonNumber.getValue())
        .isEqualTo(4);
    assertThat(updatedEpisode.episodeNumber.getValue())
        .isEqualTo(2);
  }

  @Test
  public void testEpisodeNumbersSwapped() throws SQLException, IOException, SAXException, ShowFailedException, BadlyFormattedXMLException, UnirestException, AuthenticationException {
    createSeries(SCHUMER_SERIES_NAME, SCHUMER_SERIES_ID);

    Series series = findSeriesWithTitle(SCHUMER_SERIES_NAME);

    addEpisode(series, 4, 1, SCHUMER_EPISODE_NAME1, SCHUMER_EPISODE_ID1);
    addEpisode(series, 4, 2, SCHUMER_EPISODE_NAME3, SCHUMER_EPISODE_ID3);
    addEpisode(series, 4, 3, SCHUMER_EPISODE_NAME2, SCHUMER_EPISODE_ID2);

    TVDBSeriesUpdater tvdbSeriesUpdater = new TVDBSeriesUpdater(connection, series, tvdbjwtProvider, new JSONReaderImpl());
    tvdbSeriesUpdater.updateSeries();

    TVDBEpisode tvdbEpisode = findTVDBEpisodeWithTVDBID(SCHUMER_EPISODE_ID3);

    assertThat(tvdbEpisode)
        .isNotNull();
    //noinspection ConstantConditions
    assertThat(tvdbEpisode.episodeNumber.getValue())
        .isEqualTo(3);

    TVDBEpisode thirdEpisode = findTVDBEpisodeWithTVDBID(SCHUMER_EPISODE_ID2);

    assertThat(thirdEpisode)
        .isNotNull();
    //noinspection ConstantConditions
    assertThat(thirdEpisode.episodeNumber.getValue())
        .isEqualTo(2);
  }



  @Test
  public void testAirDateDatesLinked() throws SQLException, ShowFailedException, BadlyFormattedXMLException, UnirestException, AuthenticationException {
    Series series = createSeries(SCHUMER_SERIES_NAME, SCHUMER_SERIES_ID);

    addEpisode(series, 4, 1, SCHUMER_EPISODE_NAME1, SCHUMER_EPISODE_ID1);
    Episode secondEpisode = addEpisode(series, 4, 2, SCHUMER_EPISODE_NAME2, SCHUMER_EPISODE_ID2);
    addEpisode(series, 4, 3, SCHUMER_EPISODE_NAME3, SCHUMER_EPISODE_ID3);

    Integer episodeID = secondEpisode.id.getValue();

    TVDBEpisode tvdbEpisode = secondEpisode.getTVDBEpisode(connection);

    Date originalDate = new LocalDate(2016, 2, 13).toDate();
    Date xmlDate = new LocalDate(2016, 4, 28).toDate();

    tvdbEpisode.firstAired.changeValue(originalDate);
    tvdbEpisode.commit(connection);

    secondEpisode.airDate.changeValue(originalDate);
    secondEpisode.commit(connection);

    TVDBSeriesUpdater tvdbSeriesUpdater = new TVDBSeriesUpdater(connection, series, tvdbjwtProvider, new JSONReaderImpl());
    tvdbSeriesUpdater.updateSeries();

    @NotNull Episode episode = findEpisodeWithID(episodeID);
    TVDBEpisode foundTVDBEpisode = episode.getTVDBEpisode(connection);

    assertThat(foundTVDBEpisode.firstAired.getValue().getTime())
        .isNotEqualTo(originalDate.getTime())
        .isEqualTo(xmlDate.getTime());

    assertThat(episode.airDate.getValue().getTime())
        .isNotEqualTo(originalDate.getTime())
        .isEqualTo(xmlDate.getTime());

  }



  @Test
  public void testAirDateNew() throws SQLException, ShowFailedException, BadlyFormattedXMLException, UnirestException, AuthenticationException {
    Series series = createSeries(SCHUMER_SERIES_NAME, SCHUMER_SERIES_ID);

    Date xmlDate = new LocalDate(2016, 4, 28).toDate();

    TVDBSeriesUpdater tvdbSeriesUpdater = new TVDBSeriesUpdater(connection, series, tvdbjwtProvider, new JSONReaderImpl());
    tvdbSeriesUpdater.updateSeries();

    @NotNull Episode episode = findEpisode(SCHUMER_SERIES_NAME, 4, 2);
    TVDBEpisode foundTVDBEpisode = episode.getTVDBEpisode(connection);

    assertThat(foundTVDBEpisode.firstAired.getValue().getTime())
        .isEqualTo(xmlDate.getTime());

    assertThat(episode.airDate.getValue().getTime())
        .isEqualTo(xmlDate.getTime());
  }


  @Test
  public void testAirDateOverride() throws SQLException, ShowFailedException, BadlyFormattedXMLException, UnirestException, AuthenticationException {
    Series series = createSeries(SCHUMER_SERIES_NAME, SCHUMER_SERIES_ID);

    addEpisode(series, 4, 1, SCHUMER_EPISODE_NAME1, SCHUMER_EPISODE_ID1);
    Episode secondEpisode = addEpisode(series, 4, 2, SCHUMER_EPISODE_NAME2, SCHUMER_EPISODE_ID2);
    addEpisode(series, 4, 3, SCHUMER_EPISODE_NAME3, SCHUMER_EPISODE_ID3);

    Integer episodeID = secondEpisode.id.getValue();

    TVDBEpisode tvdbEpisode = secondEpisode.getTVDBEpisode(connection);

    Date originalDate = new LocalDate(2016, 2, 13).toDate();
    Date overriddenDate = new LocalDate(2016, 6, 4).toDate();
    Date xmlDate = new LocalDate(2016, 4, 28).toDate();

    tvdbEpisode.firstAired.changeValue(originalDate);
    tvdbEpisode.commit(connection);

    secondEpisode.airDate.changeValue(overriddenDate);
    secondEpisode.commit(connection);

    TVDBSeriesUpdater tvdbSeriesUpdater = new TVDBSeriesUpdater(connection, series, tvdbjwtProvider, new JSONReaderImpl());
    tvdbSeriesUpdater.updateSeries();

    @NotNull Episode episode = findEpisodeWithID(episodeID);
    TVDBEpisode foundTVDBEpisode = episode.getTVDBEpisode(connection);

    assertThat(foundTVDBEpisode.firstAired.getValue().getTime())
        .isNotEqualTo(originalDate.getTime())
        .isEqualTo(xmlDate.getTime());

    assertThat(episode.airDate.getValue().getTime())
        .isNotEqualTo(xmlDate.getTime())
        .isEqualTo(overriddenDate.getTime());

  }

  @Test
  public void testEpisodeNumberOverride() throws SQLException, ShowFailedException, BadlyFormattedXMLException, UnirestException, AuthenticationException {
    Series series = createSeries(SCHUMER_SERIES_NAME, SCHUMER_SERIES_ID);

    addEpisode(series, 4, 1, SCHUMER_EPISODE_NAME1, SCHUMER_EPISODE_ID1);
    Episode secondEpisode = addEpisode(series, 4, 2, SCHUMER_EPISODE_NAME2, SCHUMER_EPISODE_ID2);
    addEpisode(series, 4, 3, SCHUMER_EPISODE_NAME3, SCHUMER_EPISODE_ID3);

    Integer episodeID = secondEpisode.id.getValue();

    Integer originalEpisodeNumber = 2;
    Integer overriddenEpisodeNumber = 5;

    secondEpisode.episodeNumber.changeValue(overriddenEpisodeNumber);
    secondEpisode.commit(connection);

    TVDBSeriesUpdater tvdbSeriesUpdater = new TVDBSeriesUpdater(connection, series, tvdbjwtProvider, new JSONReaderImpl());
    tvdbSeriesUpdater.updateSeries();

    @NotNull Episode episode = findEpisodeWithID(episodeID);
    TVDBEpisode foundTVDBEpisode = episode.getTVDBEpisode(connection);

    assertThat(foundTVDBEpisode.episodeNumber.getValue())
            .isNotEqualTo(overriddenEpisodeNumber)
            .isEqualTo(originalEpisodeNumber);

    assertThat(episode.episodeNumber.getValue())
            .isNotEqualTo(originalEpisodeNumber)
            .isEqualTo(overriddenEpisodeNumber);
  }

  @Test
  public void testSeasonNumberOverride() throws SQLException, ShowFailedException, BadlyFormattedXMLException, UnirestException, AuthenticationException {
    Series series = createSeries(SCHUMER_SERIES_NAME, SCHUMER_SERIES_ID);

    addEpisode(series, 4, 1, SCHUMER_EPISODE_NAME1, SCHUMER_EPISODE_ID1);
    Episode secondEpisode = addEpisode(series, 4, 2, SCHUMER_EPISODE_NAME2, SCHUMER_EPISODE_ID2);
    addEpisode(series, 4, 3, SCHUMER_EPISODE_NAME3, SCHUMER_EPISODE_ID3);



    Integer episodeID = secondEpisode.id.getValue();

    Integer originalSeasonNumber = 4;
    Integer overriddenSeasonNumber = 0;

    secondEpisode.setSeason(overriddenSeasonNumber, connection);
    secondEpisode.commit(connection);

    TVDBSeriesUpdater tvdbSeriesUpdater = new TVDBSeriesUpdater(connection, series, tvdbjwtProvider, new JSONReaderImpl());
    tvdbSeriesUpdater.updateSeries();

    @NotNull Episode episode = findEpisodeWithID(episodeID);
    TVDBEpisode foundTVDBEpisode = episode.getTVDBEpisode(connection);

    assertThat(foundTVDBEpisode.seasonNumber.getValue())
            .isNotEqualTo(overriddenSeasonNumber)
            .isEqualTo(originalSeasonNumber);

    assertThat(episode.getSeason())
            .isNotEqualTo(originalSeasonNumber)
            .isEqualTo(overriddenSeasonNumber);
  }

  @Test
  public void testPosterOverride() throws SQLException, ShowFailedException, BadlyFormattedXMLException, UnirestException, AuthenticationException {

    String originalPoster =   "posters/override.jpg";
    String overriddenPoster = "posters/original.jpg";

    // this is the filename of the last poster in 265374_posters.json, so it is used by default.
    String changedPoster =    "posters/265374-9.jpg";

    Series series = createSeries(SCHUMER_SERIES_NAME, SCHUMER_SERIES_ID);

    addEpisode(series, 4, 1, SCHUMER_EPISODE_NAME1, SCHUMER_EPISODE_ID1);
    addEpisode(series, 4, 2, SCHUMER_EPISODE_NAME2, SCHUMER_EPISODE_ID2);
    addEpisode(series, 4, 3, SCHUMER_EPISODE_NAME3, SCHUMER_EPISODE_ID3);

    TVDBSeries tvdbSeries = series.getTVDBSeries(connection).get();
    tvdbSeries.lastPoster.changeValue(originalPoster);
    tvdbSeries.commit(connection);

    series.poster.changeValue(overriddenPoster);
    series.commit(connection);

    TVDBSeriesUpdater tvdbSeriesUpdater = new TVDBSeriesUpdater(connection, series, tvdbjwtProvider, new JSONReaderImpl());
    tvdbSeriesUpdater.updateSeries();

    Series foundSeries = findSeriesWithTitle("Inside Amy Schumer");
    assertThat(foundSeries.poster.getValue())
        .as("Expected series poster to remain unchanged because it was overridden.")
        .isEqualTo(overriddenPoster);

    TVDBSeries foundTVDBSeries = foundSeries.getTVDBSeries(connection).get();
    assertThat(foundTVDBSeries.lastPoster.getValue())
        .as("Expected change to tvdb_series with new value from XML.")
        .isNotEqualTo(originalPoster)
        .isEqualTo(changedPoster);
  }


  // private methods

  private Series createSeries(String seriesName, Integer tvdbId) throws SQLException {
    TVDBSeries tvdbSeries = new TVDBSeries();
    tvdbSeries.initializeForInsert();
    tvdbSeries.tvdbSeriesExtId.changeValue(tvdbId);
    tvdbSeries.name.changeValue(seriesName);
    tvdbSeries.lastPoster.changeValue("graphical/265374-g4.jpg");
    tvdbSeries.commit(connection);

    Series series = new Series();
    series.initializeForInsert();
    series.seriesTitle.changeValue(seriesName);
    series.tvdbSeriesExtId.changeValue(tvdbId);
    series.tvdbSeriesId.changeValue(tvdbSeries.id.getValue());
    series.matchedWrong.changeValue(false);
    series.needsTVDBRedo.changeValue(false);
    series.poster.changeValue("graphical/265374-g4.jpg");
    series.commit(connection);

    return series;
  }

  private Episode addEpisode(Series series, Integer seasonNumber, Integer episodeNumber, String episodeTitle, Integer tvdbEpisodeId) throws SQLException {
    TVDBEpisode tvdbEpisode = new TVDBEpisode();
    tvdbEpisode.initializeForInsert();
    tvdbEpisode.tvdbSeriesId.changeValue(series.tvdbSeriesId.getValue());
    tvdbEpisode.seriesName.changeValue(series.seriesTitle.getValue());
    tvdbEpisode.seasonNumber.changeValue(seasonNumber);
    tvdbEpisode.episodeNumber.changeValue(episodeNumber);
    tvdbEpisode.name.changeValue(episodeTitle);
    tvdbEpisode.tvdbEpisodeExtId.changeValue(tvdbEpisodeId);
    tvdbEpisode.commit(connection);

    Episode episode = new Episode();
    episode.initializeForInsert();
    episode.seriesId.changeValue(series.id.getValue());
    episode.tvdbEpisodeId.changeValue(tvdbEpisode.id.getValue());
    episode.seriesTitle.changeValue(series.seriesTitle.getValue());
    episode.setSeason(seasonNumber, connection);
    episode.episodeNumber.changeValue(episodeNumber);
    episode.title.changeValue(episodeTitle);
    episode.commit(connection);

    return episode;
  }

  @NotNull
  private Series findSeriesWithTitle(String title) throws SQLException {
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(
        "SELECT * " +
            "FROM series " +
            "WHERE title = ? " +
            "and retired = ? ",
        title, 0
    );
    if (resultSet.next()) {
      Series series = new Series();
      series.initializeFromDBObject(resultSet);
      return series;
    } else {
      throw new IllegalStateException("Unable to find series.");
    }
  }

  @Nullable
  private TVDBEpisode findTVDBEpisodeWithTVDBID(Integer tvdbId) throws SQLException {
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(
        "SELECT * " +
            "FROM tvdb_episode " +
            "WHERE tvdb_episode_ext_id = ?", tvdbId
    );
    if (resultSet.next()) {
      TVDBEpisode tvdbEpisode = new TVDBEpisode();
      tvdbEpisode.initializeFromDBObject(resultSet);
      return tvdbEpisode;
    } else {
      return null;
    }
  }

  @NotNull
  private Episode findEpisodeWithID(Integer episodeID) throws SQLException {
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(
        "SELECT * " +
            "FROM episode " +
            "WHERE id = ?", episodeID
    );
    if (resultSet.next()) {
      Episode episode = new Episode();
      episode.initializeFromDBObject(resultSet);
      return episode;
    } else {
      fail();
      throw new RuntimeException("Blah");
    }
  }

  @NotNull
  private Episode findEpisode(String seriesName, Integer seasonNumber, Integer episodeNumber) throws SQLException {
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(
        "SELECT * " +
            "FROM episode " +
            "WHERE series_title = ? " +
            "AND season = ? " +
            "AND episode_number = ? ", seriesName, seasonNumber, episodeNumber
    );
    if (resultSet.next()) {
      Episode episode = new Episode();
      episode.initializeFromDBObject(resultSet);
      return episode;
    } else {
      fail();
      throw new RuntimeException("Blah");
    }
  }
}