package com.mayhew3.mediamogul.tv;

import com.mashape.unirest.http.exceptions.UnirestException;
import com.mayhew3.mediamogul.DatabaseTest;
import com.mayhew3.mediamogul.exception.MissingEnvException;
import com.mayhew3.mediamogul.model.Person;
import com.mayhew3.mediamogul.model.tv.*;
import com.mayhew3.mediamogul.socket.SocketWrapper;
import com.mayhew3.mediamogul.tv.exception.ShowFailedException;
import com.mayhew3.mediamogul.tv.helper.TVDBApprovalStatus;
import com.mayhew3.mediamogul.tv.provider.TVDBJWTProvider;
import com.mayhew3.mediamogul.tv.provider.TVDBLocalJSONProvider;
import com.mayhew3.mediamogul.xml.JSONReaderImpl;
import org.apache.http.auth.AuthenticationException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.json.JSONObject;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

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

  private SocketWrapper socket;

  private Date SCHUMER_EPISODE_AIR1;
  private Date SCHUMER_EPISODE_AIR2;
  private Date SCHUMER_EPISODE_AIR3;

  private TVDBJWTProvider tvdbjwtProvider;

  @Override
  public void setUp() throws URISyntaxException, SQLException, MissingEnvException {
    super.setUp();
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.m");
    try {
      SCHUMER_EPISODE_AIR1 = dateFormat.parse("2016-04-21 22:00:00.0");
      SCHUMER_EPISODE_AIR2 = dateFormat.parse("2016-04-28 22:00:00.0");
      SCHUMER_EPISODE_AIR3 = dateFormat.parse("2016-05-05 22:00:00.0");
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
    tvdbjwtProvider = new TVDBLocalJSONProvider("src\\test\\resources\\TVDBTest\\");
    socket = mock(SocketWrapper.class);
  }

  @Test
  public void testIDChangedForTVDBEpisode() throws SQLException, ShowFailedException, UnirestException, AuthenticationException {
    Person person = addPerson();

    createSeries(SCHUMER_SERIES_NAME, SCHUMER_SERIES_ID);

    Series series = findSeriesWithTitle(SCHUMER_SERIES_NAME);

    // fake ID - use so that XML will find a different ID and change it and not add a new episode with same episode number.
    Integer originalID = 5;

    addEpisode(series, 4, 1, SCHUMER_EPISODE_NAME1, SCHUMER_EPISODE_ID1);
    Episode secondEpisode = addEpisode(series, 4, 2, SCHUMER_EPISODE_NAME2, originalID);
    addEpisode(series, 4, 3, SCHUMER_EPISODE_NAME3, SCHUMER_EPISODE_ID3);

    EpisodeRating secondRating = addRating(secondEpisode, person.id.getValue());

    TVDBSeriesUpdater tvdbSeriesUpdater = new TVDBSeriesUpdater(connection, series, tvdbjwtProvider, new JSONReaderImpl(), socket);
    tvdbSeriesUpdater.updateSeries();

    TVDBEpisode retiredTVDBEpisode = findTVDBEpisodeWithTVDBID(originalID);
    assertThat(retiredTVDBEpisode)
        .isNotNull();
    //noinspection ConstantConditions
    assertThat(retiredTVDBEpisode.retired.getValue())
        .isNotEqualTo(0);
    assertThat(retiredTVDBEpisode.seasonNumber.getValue())
        .isEqualTo(4);
    assertThat(retiredTVDBEpisode.episodeNumber.getValue())
        .isEqualTo(2);

    TVDBEpisode updatedTVDBEpisode = findTVDBEpisodeWithTVDBID(SCHUMER_EPISODE_ID2);
    assertThat(updatedTVDBEpisode)
        .isNotNull();
    //noinspection ConstantConditions
    assertThat(updatedTVDBEpisode.retired.getValue())
        .isEqualTo(0);
    assertThat(updatedTVDBEpisode.seasonNumber.getValue())
        .isEqualTo(4);
    assertThat(updatedTVDBEpisode.episodeNumber.getValue())
        .isEqualTo(2);

    Episode retiredEpisode = getRetiredEpisode(secondEpisode.id.getValue());
    List<EpisodeRating> originalRatings = retiredEpisode.getEpisodeRatings(connection);
    assertThat(originalRatings)
        .isEmpty();

    Episode updatedEpisode = updatedTVDBEpisode.getEpisode(connection);
    List<EpisodeRating> updatedRatings = updatedEpisode.getEpisodeRatings(connection);
    assertThat(updatedRatings)
        .hasSize(1);
    EpisodeRating episodeRating = updatedRatings.get(0);
    assertThat(episodeRating.id.getValue())
        .isEqualTo(secondRating.id.getValue());

    verifyZeroInteractions(socket);
  }

  @Test
  public void testEpisodeFlaggedWhenRatingExists() throws SQLException, ShowFailedException, UnirestException, AuthenticationException {
    Person person = addPerson();

    createSeries(SCHUMER_SERIES_NAME, SCHUMER_SERIES_ID);

    Series series = findSeriesWithTitle(SCHUMER_SERIES_NAME);

    Episode firstEpisode = addEpisodeWithAirTime(series, 4, 1, SCHUMER_EPISODE_NAME1, SCHUMER_EPISODE_ID1, SCHUMER_EPISODE_AIR1);
    Episode thirdEpisode = addEpisodeWithAirTime(series, 4, 3, SCHUMER_EPISODE_NAME3, SCHUMER_EPISODE_ID3, SCHUMER_EPISODE_AIR3);

    addRating(firstEpisode, person.id.getValue());
    addRating(thirdEpisode, person.id.getValue());

    TVDBSeriesUpdater tvdbSeriesUpdater = new TVDBSeriesUpdater(connection, series, tvdbjwtProvider, new JSONReaderImpl(), socket);
    tvdbSeriesUpdater.updateSeries();

    firstEpisode = findEpisode(SCHUMER_SERIES_NAME, 4, 1);
    Episode secondEpisode = findEpisode(SCHUMER_SERIES_NAME, 4, 2);
    thirdEpisode = findEpisode(SCHUMER_SERIES_NAME, 4, 3);

    assertThat(firstEpisode.tvdbApproval.getValue())
        .isEqualToIgnoringCase("approved");
    assertThat(secondEpisode.tvdbApproval.getValue())
        .isEqualToIgnoringCase("pending");
    assertThat(thirdEpisode.tvdbApproval.getValue())
        .isEqualToIgnoringCase("approved");

    ArgumentCaptor<String> stringCaptor = ArgumentCaptor.forClass(String.class);
    verify(socket).emit(stringCaptor.capture(), any(JSONObject.class));

    String capturedChannel = stringCaptor.getValue();

    assertThat(capturedChannel).isEqualToIgnoringCase("tvdb_pending");
  }

  @Test
  public void testEpisodeNotUnflaggedOnUpdate() throws SQLException, ShowFailedException, UnirestException, AuthenticationException {
    Person person = addPerson();

    createSeries(SCHUMER_SERIES_NAME, SCHUMER_SERIES_ID);

    Series series = findSeriesWithTitle(SCHUMER_SERIES_NAME);

    Episode firstEpisode = addEpisodeWithAirTime(series, 4, 1, SCHUMER_EPISODE_NAME1, SCHUMER_EPISODE_ID1, SCHUMER_EPISODE_AIR1);
    Episode secondEpisode = addEpisodeWithAirTime(series, 4, 2, SCHUMER_EPISODE_NAME2, SCHUMER_EPISODE_ID2, SCHUMER_EPISODE_AIR2);
    Episode thirdEpisode = addEpisodeWithAirTime(series, 4, 3, SCHUMER_EPISODE_NAME3, SCHUMER_EPISODE_ID3, SCHUMER_EPISODE_AIR3);

    secondEpisode.tvdbApproval.changeValue(TVDBApprovalStatus.PENDING.getTypeKey());
    secondEpisode.commit(connection);

    addRating(firstEpisode, person.id.getValue());
    addRating(thirdEpisode, person.id.getValue());

    TVDBSeriesUpdater tvdbSeriesUpdater = new TVDBSeriesUpdater(connection, series, tvdbjwtProvider, new JSONReaderImpl(), socket);
    tvdbSeriesUpdater.updateSeries();

    firstEpisode = findEpisode(SCHUMER_SERIES_NAME, 4, 1);
    secondEpisode = findEpisode(SCHUMER_SERIES_NAME, 4, 2);
    thirdEpisode = findEpisode(SCHUMER_SERIES_NAME, 4, 3);

    assertThat(firstEpisode.tvdbApproval.getValue())
        .isEqualToIgnoringCase("approved");
    assertThat(secondEpisode.tvdbApproval.getValue())
        .isEqualToIgnoringCase("pending");
    assertThat(thirdEpisode.tvdbApproval.getValue())
        .isEqualToIgnoringCase("approved");

    verifyZeroInteractions(socket);
  }

  @Test
  public void testEpisodeUnFlaggedWhenSeasonChanged() throws SQLException, ShowFailedException, UnirestException, AuthenticationException {
    Person person = addPerson();

    TVDBLocalJSONProvider tvdbLocalJSONProvider = new TVDBLocalJSONProvider("src\\test\\resources\\TVDBSeasonZero\\");

    createSeries(SCHUMER_SERIES_NAME, SCHUMER_SERIES_ID);

    Series series = findSeriesWithTitle(SCHUMER_SERIES_NAME);

    Episode firstEpisode = addEpisodeWithAirTime(series, 4, 1, SCHUMER_EPISODE_NAME1, SCHUMER_EPISODE_ID1, SCHUMER_EPISODE_AIR1);
    Episode secondEpisode = addEpisodeWithAirTime(series, 4, 2, SCHUMER_EPISODE_NAME2, SCHUMER_EPISODE_ID2, SCHUMER_EPISODE_AIR2);
    Episode thirdEpisode = addEpisodeWithAirTime(series, 4, 3, SCHUMER_EPISODE_NAME3, SCHUMER_EPISODE_ID3, SCHUMER_EPISODE_AIR3);

    addRating(firstEpisode, person.id.getValue());
    addRating(thirdEpisode, person.id.getValue());

    secondEpisode.tvdbApproval.changeValue(TVDBApprovalStatus.PENDING.getTypeKey());
    secondEpisode.commit(connection);

    TVDBSeriesUpdater tvdbSeriesUpdater = new TVDBSeriesUpdater(connection, series, tvdbLocalJSONProvider, new JSONReaderImpl(), socket);
    tvdbSeriesUpdater.updateSeries();

    firstEpisode = findEpisode(SCHUMER_SERIES_NAME, 4, 1);
    secondEpisode = findEpisode(SCHUMER_SERIES_NAME, 0, 8);
    thirdEpisode = findEpisode(SCHUMER_SERIES_NAME, 4, 3);

    assertThat(firstEpisode.tvdbApproval.getValue())
        .isEqualToIgnoringCase("approved");
    assertThat(secondEpisode.tvdbApproval.getValue())
        .isEqualToIgnoringCase("approved");
    assertThat(thirdEpisode.tvdbApproval.getValue())
        .isEqualToIgnoringCase("approved");

    ArgumentCaptor<String> stringCaptor = ArgumentCaptor.forClass(String.class);
    verify(socket).emit(stringCaptor.capture(), any(JSONObject.class));

    String capturedChannel = stringCaptor.getValue();

    assertThat(capturedChannel).isEqualToIgnoringCase("tvdb_episode_resolve");
  }

  @Test
  public void testEpisodeUnFlaggedWhenAirDateChanged() throws SQLException, ShowFailedException, UnirestException, AuthenticationException {
    Person person = addPerson();

    TVDBLocalJSONProvider tvdbLocalJSONProvider = new TVDBLocalJSONProvider("src\\test\\resources\\TVDBFutureDate\\");

    createSeries(SCHUMER_SERIES_NAME, SCHUMER_SERIES_ID);

    Series series = findSeriesWithTitle(SCHUMER_SERIES_NAME);

    Episode firstEpisode = addEpisodeWithAirTime(series, 4, 1, SCHUMER_EPISODE_NAME1, SCHUMER_EPISODE_ID1, SCHUMER_EPISODE_AIR1);
    Episode secondEpisode = addEpisodeWithAirTime(series, 4, 2, SCHUMER_EPISODE_NAME2, SCHUMER_EPISODE_ID2, SCHUMER_EPISODE_AIR2);
    Episode thirdEpisode = addEpisodeWithAirTime(series, 4, 3, SCHUMER_EPISODE_NAME3, SCHUMER_EPISODE_ID3, SCHUMER_EPISODE_AIR3);

    addRating(firstEpisode, person.id.getValue());
    addRating(thirdEpisode, person.id.getValue());

    secondEpisode.tvdbApproval.changeValue(TVDBApprovalStatus.PENDING.getTypeKey());
    secondEpisode.commit(connection);

    TVDBSeriesUpdater tvdbSeriesUpdater = new TVDBSeriesUpdater(connection, series, tvdbLocalJSONProvider, new JSONReaderImpl(), socket);
    tvdbSeriesUpdater.updateSeries();

    firstEpisode = findEpisode(SCHUMER_SERIES_NAME, 4, 1);
    secondEpisode = findEpisode(SCHUMER_SERIES_NAME, 4, 2);
    thirdEpisode = findEpisode(SCHUMER_SERIES_NAME, 4, 3);

    assertThat(firstEpisode.tvdbApproval.getValue())
        .isEqualToIgnoringCase("approved");
    assertThat(secondEpisode.tvdbApproval.getValue())
        .isEqualToIgnoringCase("approved");
    assertThat(thirdEpisode.tvdbApproval.getValue())
        .isEqualToIgnoringCase("approved");

    ArgumentCaptor<String> stringCaptor = ArgumentCaptor.forClass(String.class);
    verify(socket).emit(stringCaptor.capture(), any(JSONObject.class));

    String capturedChannel = stringCaptor.getValue();

    assertThat(capturedChannel).isEqualToIgnoringCase("tvdb_episode_resolve");
  }

  @Test
  public void testEpisodeReFlaggedWhenSeasonChanged() throws SQLException, ShowFailedException, UnirestException, AuthenticationException {
    Person person = addPerson();

    createSeries(SCHUMER_SERIES_NAME, SCHUMER_SERIES_ID);

    Series series = findSeriesWithTitle(SCHUMER_SERIES_NAME);

    Episode firstEpisode = addEpisodeWithAirTime(series, 4, 1, SCHUMER_EPISODE_NAME1, SCHUMER_EPISODE_ID1, SCHUMER_EPISODE_AIR1);
    addEpisodeWithAirTime(series, 0, 8, SCHUMER_EPISODE_NAME2, SCHUMER_EPISODE_ID2, SCHUMER_EPISODE_AIR2);
    Episode thirdEpisode = addEpisodeWithAirTime(series, 4, 3, SCHUMER_EPISODE_NAME3, SCHUMER_EPISODE_ID3, SCHUMER_EPISODE_AIR3);

    addRating(firstEpisode, person.id.getValue());
    addRating(thirdEpisode, person.id.getValue());

    TVDBSeriesUpdater tvdbSeriesUpdater = new TVDBSeriesUpdater(connection, series, tvdbjwtProvider, new JSONReaderImpl(), socket);
    tvdbSeriesUpdater.updateSeries();

    firstEpisode = findEpisode(SCHUMER_SERIES_NAME, 4, 1);
    Episode secondEpisode = findEpisode(SCHUMER_SERIES_NAME, 4, 2);
    thirdEpisode = findEpisode(SCHUMER_SERIES_NAME, 4, 3);

    assertThat(firstEpisode.tvdbApproval.getValue())
        .isEqualToIgnoringCase("approved");
    assertThat(secondEpisode.tvdbApproval.getValue())
        .isEqualToIgnoringCase("pending");
    assertThat(thirdEpisode.tvdbApproval.getValue())
        .isEqualToIgnoringCase("approved");

    ArgumentCaptor<String> stringCaptor = ArgumentCaptor.forClass(String.class);
    verify(socket).emit(stringCaptor.capture(), any(JSONObject.class));

    String capturedChannel = stringCaptor.getValue();

    assertThat(capturedChannel).isEqualToIgnoringCase("tvdb_pending");
  }

  @Test
  public void testEpisodeReFlaggedWhenAirDateChanged() throws SQLException, ShowFailedException, UnirestException, AuthenticationException {
    Person person = addPerson();

    Timestamp futureDate = new Timestamp(DateTime.now().plusDays(7).toDate().getTime());

    createSeries(SCHUMER_SERIES_NAME, SCHUMER_SERIES_ID);

    Series series = findSeriesWithTitle(SCHUMER_SERIES_NAME);

    Episode firstEpisode = addEpisodeWithAirTime(series, 4, 1, SCHUMER_EPISODE_NAME1, SCHUMER_EPISODE_ID1, SCHUMER_EPISODE_AIR1);
    addEpisodeWithAirTime(series, 4, 2, SCHUMER_EPISODE_NAME2, SCHUMER_EPISODE_ID2, futureDate);
    Episode thirdEpisode = addEpisodeWithAirTime(series, 4, 3, SCHUMER_EPISODE_NAME3, SCHUMER_EPISODE_ID3, SCHUMER_EPISODE_AIR3);

    addRating(firstEpisode, person.id.getValue());
    addRating(thirdEpisode, person.id.getValue());

    TVDBSeriesUpdater tvdbSeriesUpdater = new TVDBSeriesUpdater(connection, series, tvdbjwtProvider, new JSONReaderImpl(), socket);
    tvdbSeriesUpdater.updateSeries();

    firstEpisode = findEpisode(SCHUMER_SERIES_NAME, 4, 1);
    Episode secondEpisode = findEpisode(SCHUMER_SERIES_NAME, 4, 2);
    thirdEpisode = findEpisode(SCHUMER_SERIES_NAME, 4, 3);

    assertThat(firstEpisode.tvdbApproval.getValue())
        .isEqualToIgnoringCase("approved");
    assertThat(secondEpisode.tvdbApproval.getValue())
        .isEqualToIgnoringCase("pending");
    assertThat(thirdEpisode.tvdbApproval.getValue())
        .isEqualToIgnoringCase("approved");

    ArgumentCaptor<String> stringCaptor = ArgumentCaptor.forClass(String.class);
    verify(socket).emit(stringCaptor.capture(), any(JSONObject.class));

    String capturedChannel = stringCaptor.getValue();

    assertThat(capturedChannel).isEqualToIgnoringCase("tvdb_pending");
  }

  @Test
  public void testEpisodeNotFlaggedWhenNoRatingsExists() throws SQLException, ShowFailedException, UnirestException, AuthenticationException {
    createSeries(SCHUMER_SERIES_NAME, SCHUMER_SERIES_ID);

    Series series = findSeriesWithTitle(SCHUMER_SERIES_NAME);

    addEpisodeWithAirTime(series, 4, 1, SCHUMER_EPISODE_NAME1, SCHUMER_EPISODE_ID1, SCHUMER_EPISODE_AIR1);
    addEpisodeWithAirTime(series, 4, 3, SCHUMER_EPISODE_NAME3, SCHUMER_EPISODE_ID3, SCHUMER_EPISODE_AIR3);

    TVDBSeriesUpdater tvdbSeriesUpdater = new TVDBSeriesUpdater(connection, series, tvdbjwtProvider, new JSONReaderImpl(), socket);
    tvdbSeriesUpdater.updateSeries();

    Episode firstEpisode = findEpisode(SCHUMER_SERIES_NAME, 4, 1);
    Episode secondEpisode = findEpisode(SCHUMER_SERIES_NAME, 4, 2);
    Episode thirdEpisode = findEpisode(SCHUMER_SERIES_NAME, 4, 3);

    assertThat(firstEpisode.tvdbApproval.getValue())
        .isEqualToIgnoringCase("approved");
    assertThat(secondEpisode.tvdbApproval.getValue())
        .isEqualToIgnoringCase("approved");
    assertThat(thirdEpisode.tvdbApproval.getValue())
        .isEqualToIgnoringCase("approved");

    verifyZeroInteractions(socket);
  }

  @Test
  public void testEpisodeNotFlaggedWhenOnlyEarlierRatingsExists() throws SQLException, ShowFailedException, UnirestException, AuthenticationException {
    Person person = addPerson();

    createSeries(SCHUMER_SERIES_NAME, SCHUMER_SERIES_ID);

    Series series = findSeriesWithTitle(SCHUMER_SERIES_NAME);

    Episode firstEpisode = addEpisodeWithAirTime(series, 4, 1, SCHUMER_EPISODE_NAME1, SCHUMER_EPISODE_ID1, SCHUMER_EPISODE_AIR1);
    addEpisodeWithAirTime(series, 4, 3, SCHUMER_EPISODE_NAME3, SCHUMER_EPISODE_ID3, SCHUMER_EPISODE_AIR3);

    addRating(firstEpisode, person.id.getValue());

    TVDBSeriesUpdater tvdbSeriesUpdater = new TVDBSeriesUpdater(connection, series, tvdbjwtProvider, new JSONReaderImpl(), socket);
    tvdbSeriesUpdater.updateSeries();

    firstEpisode = findEpisode(SCHUMER_SERIES_NAME, 4, 1);
    Episode secondEpisode = findEpisode(SCHUMER_SERIES_NAME, 4, 2);
    Episode thirdEpisode = findEpisode(SCHUMER_SERIES_NAME, 4, 3);

    assertThat(firstEpisode.tvdbApproval.getValue())
        .isEqualToIgnoringCase("approved");
    assertThat(secondEpisode.tvdbApproval.getValue())
        .isEqualToIgnoringCase("approved");
    assertThat(thirdEpisode.tvdbApproval.getValue())
        .isEqualToIgnoringCase("approved");

    verifyZeroInteractions(socket);
  }

  @Test
  public void testEpisodeNumbersSwapped() throws SQLException, ShowFailedException, UnirestException, AuthenticationException {
    createSeries(SCHUMER_SERIES_NAME, SCHUMER_SERIES_ID);

    Series series = findSeriesWithTitle(SCHUMER_SERIES_NAME);

    addEpisode(series, 4, 1, SCHUMER_EPISODE_NAME1, SCHUMER_EPISODE_ID1);
    addEpisode(series, 4, 2, SCHUMER_EPISODE_NAME3, SCHUMER_EPISODE_ID3);
    addEpisode(series, 4, 3, SCHUMER_EPISODE_NAME2, SCHUMER_EPISODE_ID2);

    TVDBSeriesUpdater tvdbSeriesUpdater = new TVDBSeriesUpdater(connection, series, tvdbjwtProvider, new JSONReaderImpl(), socket);
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

    verifyZeroInteractions(socket);
  }



  @Test
  public void testAirDateDatesLinked() throws SQLException, ShowFailedException, UnirestException, AuthenticationException {
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

    TVDBSeriesUpdater tvdbSeriesUpdater = new TVDBSeriesUpdater(connection, series, tvdbjwtProvider, new JSONReaderImpl(), socket);
    tvdbSeriesUpdater.updateSeries();

    @NotNull Episode episode = findEpisodeWithID(episodeID);
    TVDBEpisode foundTVDBEpisode = episode.getTVDBEpisode(connection);

    assertThat(foundTVDBEpisode.firstAired.getValue().getTime())
        .isNotEqualTo(originalDate.getTime())
        .isEqualTo(xmlDate.getTime());

    assertThat(episode.airDate.getValue().getTime())
        .isNotEqualTo(originalDate.getTime())
        .isEqualTo(xmlDate.getTime());

    verifyZeroInteractions(socket);
  }



  @Test
  public void testAirDateNew() throws SQLException, ShowFailedException, UnirestException, AuthenticationException {
    Series series = createSeries(SCHUMER_SERIES_NAME, SCHUMER_SERIES_ID);

    Date xmlDate = new LocalDate(2016, 4, 28).toDate();

    TVDBSeriesUpdater tvdbSeriesUpdater = new TVDBSeriesUpdater(connection, series, tvdbjwtProvider, new JSONReaderImpl(), socket);
    tvdbSeriesUpdater.updateSeries();

    @NotNull Episode episode = findEpisode(SCHUMER_SERIES_NAME, 4, 2);
    TVDBEpisode foundTVDBEpisode = episode.getTVDBEpisode(connection);

    assertThat(foundTVDBEpisode.firstAired.getValue().getTime())
        .isEqualTo(xmlDate.getTime());

    assertThat(episode.airDate.getValue().getTime())
        .isEqualTo(xmlDate.getTime());

    verifyZeroInteractions(socket);
  }


  @Test
  public void testAirDateOverride() throws SQLException, ShowFailedException, UnirestException, AuthenticationException {
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

    TVDBSeriesUpdater tvdbSeriesUpdater = new TVDBSeriesUpdater(connection, series, tvdbjwtProvider, new JSONReaderImpl(), socket);
    tvdbSeriesUpdater.updateSeries();

    @NotNull Episode episode = findEpisodeWithID(episodeID);
    TVDBEpisode foundTVDBEpisode = episode.getTVDBEpisode(connection);

    assertThat(foundTVDBEpisode.firstAired.getValue().getTime())
        .isNotEqualTo(originalDate.getTime())
        .isEqualTo(xmlDate.getTime());

    assertThat(episode.airDate.getValue().getTime())
        .isNotEqualTo(xmlDate.getTime())
        .isEqualTo(overriddenDate.getTime());

    verifyZeroInteractions(socket);
  }

  @Test
  public void testEpisodeNumberOverride() throws SQLException, ShowFailedException, UnirestException, AuthenticationException {
    Series series = createSeries(SCHUMER_SERIES_NAME, SCHUMER_SERIES_ID);

    addEpisode(series, 4, 1, SCHUMER_EPISODE_NAME1, SCHUMER_EPISODE_ID1);
    Episode secondEpisode = addEpisode(series, 4, 2, SCHUMER_EPISODE_NAME2, SCHUMER_EPISODE_ID2);
    addEpisode(series, 4, 3, SCHUMER_EPISODE_NAME3, SCHUMER_EPISODE_ID3);

    Integer episodeID = secondEpisode.id.getValue();

    Integer originalEpisodeNumber = 2;
    Integer overriddenEpisodeNumber = 5;

    secondEpisode.episodeNumber.changeValue(overriddenEpisodeNumber);
    secondEpisode.commit(connection);

    TVDBSeriesUpdater tvdbSeriesUpdater = new TVDBSeriesUpdater(connection, series, tvdbjwtProvider, new JSONReaderImpl(), socket);
    tvdbSeriesUpdater.updateSeries();

    @NotNull Episode episode = findEpisodeWithID(episodeID);
    TVDBEpisode foundTVDBEpisode = episode.getTVDBEpisode(connection);

    assertThat(foundTVDBEpisode.episodeNumber.getValue())
            .isNotEqualTo(overriddenEpisodeNumber)
            .isEqualTo(originalEpisodeNumber);

    assertThat(episode.episodeNumber.getValue())
            .isNotEqualTo(originalEpisodeNumber)
            .isEqualTo(overriddenEpisodeNumber);

    verifyZeroInteractions(socket);
  }

  @Test
  public void testSeasonNumberOverride() throws SQLException, ShowFailedException, UnirestException, AuthenticationException {
    Series series = createSeries(SCHUMER_SERIES_NAME, SCHUMER_SERIES_ID);

    addEpisode(series, 4, 1, SCHUMER_EPISODE_NAME1, SCHUMER_EPISODE_ID1);
    Episode secondEpisode = addEpisode(series, 4, 2, SCHUMER_EPISODE_NAME2, SCHUMER_EPISODE_ID2);
    addEpisode(series, 4, 3, SCHUMER_EPISODE_NAME3, SCHUMER_EPISODE_ID3);



    Integer episodeID = secondEpisode.id.getValue();

    Integer originalSeasonNumber = 4;
    Integer overriddenSeasonNumber = 0;

    secondEpisode.setSeason(overriddenSeasonNumber, connection);
    secondEpisode.commit(connection);

    TVDBSeriesUpdater tvdbSeriesUpdater = new TVDBSeriesUpdater(connection, series, tvdbjwtProvider, new JSONReaderImpl(), socket);
    tvdbSeriesUpdater.updateSeries();

    @NotNull Episode episode = findEpisodeWithID(episodeID);
    TVDBEpisode foundTVDBEpisode = episode.getTVDBEpisode(connection);

    assertThat(foundTVDBEpisode.seasonNumber.getValue())
            .isNotEqualTo(overriddenSeasonNumber)
            .isEqualTo(originalSeasonNumber);

    assertThat(episode.getSeason())
            .isNotEqualTo(originalSeasonNumber)
            .isEqualTo(overriddenSeasonNumber);

    verifyZeroInteractions(socket);
  }

  @Test
  public void testPosterOverride() throws SQLException, ShowFailedException, UnirestException, AuthenticationException {

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

    TVDBSeriesUpdater tvdbSeriesUpdater = new TVDBSeriesUpdater(connection, series, tvdbjwtProvider, new JSONReaderImpl(), socket);
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

    verifyZeroInteractions(socket);
  }

  @Test
  public void testEpisodeRemoved() throws SQLException, ShowFailedException, UnirestException, AuthenticationException {
    createSeries(SCHUMER_SERIES_NAME, SCHUMER_SERIES_ID);

    Series series = findSeriesWithTitle(SCHUMER_SERIES_NAME);

    String SCHUMER_EPISODE_NAME4 = "Not a Real Episode";
    int SCHUMER_EPISODE_ID4 = 847209;

    addEpisode(series, 4, 1, SCHUMER_EPISODE_NAME1, SCHUMER_EPISODE_ID1);
    addEpisode(series, 4, 2, SCHUMER_EPISODE_NAME2, SCHUMER_EPISODE_ID2);
    addEpisode(series, 4, 3, SCHUMER_EPISODE_NAME3, SCHUMER_EPISODE_ID3);
    Episode epFour = addEpisode(series, 4, 4, SCHUMER_EPISODE_NAME4, SCHUMER_EPISODE_ID4);
    Integer originalId = epFour.id.getValue();

    TVDBSeriesUpdater tvdbSeriesUpdater = new TVDBSeriesUpdater(connection, series, tvdbjwtProvider, new JSONReaderImpl(), socket);
    tvdbSeriesUpdater.updateSeries();

    TVDBEpisode tvdbEpisode = findTVDBEpisodeWithTVDBID(SCHUMER_EPISODE_ID4);
    assertThat(tvdbEpisode)
        .isNotNull();
    //noinspection ConstantConditions
    assertThat(tvdbEpisode.retired.getValue())
        .isNotEqualTo(0);

    Episode foundEpisode = getRetiredEpisode(originalId);

    assertThat(foundEpisode.tvdbEpisodeId.getValue())
        .isEqualTo(tvdbEpisode.id.getValue());
    assertThat(foundEpisode.retired.getValue())
        .isNotEqualTo(0);

    verifyZeroInteractions(socket);
  }

  @Test
  public void testInvalidResponseDoesntRemoveEpisodes() throws SQLException, ShowFailedException, UnirestException, AuthenticationException {
    createSeries(SCHUMER_SERIES_NAME, SCHUMER_SERIES_ID);

    TVDBLocalJSONProvider tvdbLocalJSONProvider = new TVDBLocalJSONProvider("src\\test\\resources\\TVDBBrokenTest\\");

    Series series = findSeriesWithTitle(SCHUMER_SERIES_NAME);

    addEpisode(series, 4, 1, SCHUMER_EPISODE_NAME1, SCHUMER_EPISODE_ID1);
    addEpisode(series, 4, 2, SCHUMER_EPISODE_NAME2, SCHUMER_EPISODE_ID2);
    addEpisode(series, 4, 3, SCHUMER_EPISODE_NAME3, SCHUMER_EPISODE_ID3);

    TVDBSeriesUpdater tvdbSeriesUpdater = new TVDBSeriesUpdater(connection, series, tvdbLocalJSONProvider, new JSONReaderImpl(), socket);
    tvdbSeriesUpdater.updateSeries();

    TVDBEpisode tvdbEpisode = findTVDBEpisodeWithTVDBID(SCHUMER_EPISODE_ID3);
    assertThat(tvdbEpisode)
        .isNotNull();
    //noinspection ConstantConditions
    assertThat(tvdbEpisode.retired.getValue())
        .isEqualTo(0);

    Episode episode = tvdbEpisode.getEpisode(connection);
    assertThat(episode)
        .isNotNull();
    assertThat(episode.retired.getValue())
        .isEqualTo(0);

    verifyZeroInteractions(socket);
  }

  @Test
  public void testSuddenEmptyEpisodeList() throws SQLException, ShowFailedException, UnirestException, AuthenticationException {
    createSeries(SCHUMER_SERIES_NAME, SCHUMER_SERIES_ID);

    TVDBLocalJSONProvider tvdbLocalJSONProvider = new TVDBLocalJSONProvider("src\\test\\resources\\TVDBEmptyTest\\");

    Series series = findSeriesWithTitle(SCHUMER_SERIES_NAME);

    addEpisode(series, 4, 1, SCHUMER_EPISODE_NAME1, SCHUMER_EPISODE_ID1);
    addEpisode(series, 4, 2, SCHUMER_EPISODE_NAME2, SCHUMER_EPISODE_ID2);
    addEpisode(series, 4, 3, SCHUMER_EPISODE_NAME3, SCHUMER_EPISODE_ID3);

    TVDBSeriesUpdater tvdbSeriesUpdater = new TVDBSeriesUpdater(connection, series, tvdbLocalJSONProvider, new JSONReaderImpl(), socket);
    tvdbSeriesUpdater.updateSeries();

    TVDBEpisode tvdbEpisode = findTVDBEpisodeWithTVDBID(SCHUMER_EPISODE_ID3);
    assertThat(tvdbEpisode)
        .isNotNull();
    //noinspection ConstantConditions
    assertThat(tvdbEpisode.retired.getValue())
        .isEqualTo(0);

    Episode episode = tvdbEpisode.getEpisode(connection);
    assertThat(episode)
        .isNotNull();
    assertThat(episode.retired.getValue())
        .isEqualTo(0);

    verifyZeroInteractions(socket);
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
    episode.tvdbApproval.changeValue("approved");
    episode.commit(connection);

    return episode;
  }

  private Episode addEpisodeWithAirTime(Series series, Integer seasonNumber, Integer episodeNumber, String episodeTitle, Integer tvdbEpisodeId, Date airTime) throws SQLException {
    Episode episode = addEpisode(series, seasonNumber, episodeNumber, episodeTitle, tvdbEpisodeId);
    episode.airTime.changeValue(airTime);
    episode.commit(connection);
    return episode;
  }

  private Person addPerson() throws SQLException {
    Person person = new Person();
    person.initializeForInsert();
    person.email.changeValue("test@test.test");
    person.firstName.changeValue("Kylo");
    person.lastName.changeValue("Ren");
    person.userRole.changeValue("admin");
    person.commit(connection);
    return person;
  }

  private EpisodeRating addRating(Episode episode, Integer personId) throws SQLException {
    EpisodeRating episodeRating = new EpisodeRating();
    episodeRating.initializeForInsert();
    episodeRating.episodeId.changeValue(episode.id.getValue());
    episodeRating.personId.changeValue(personId);
    episodeRating.watched.changeValue(true);
    episodeRating.watchedDate.changeValue(new Date());
    episodeRating.ratingValue.changeValue(42.0);
    episodeRating.commit(connection);
    return episodeRating;
  }

  private Episode getRetiredEpisode(int episode_id) throws SQLException {
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(
        "SELECT * " +
            "FROM episode " +
            "WHERE id = ? " +
            "AND retired <> ?",
        episode_id,
        0
    );

    if (!resultSet.next()) {
      throw new IllegalStateException("No episode found with id of " + episode_id);
    }
    Episode episode = new Episode();
    episode.initializeFromDBObject(resultSet);
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