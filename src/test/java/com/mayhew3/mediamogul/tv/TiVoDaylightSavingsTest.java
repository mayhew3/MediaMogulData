package com.mayhew3.mediamogul.tv;

import com.mayhew3.mediamogul.DatabaseTest;
import com.mayhew3.mediamogul.dataobject.TiVoLocalProvider;
import com.mayhew3.mediamogul.exception.MissingEnvException;
import com.mayhew3.mediamogul.model.tv.Series;
import com.mayhew3.mediamogul.model.tv.TiVoEpisode;
import com.mayhew3.mediamogul.tv.helper.UpdateMode;
import com.mayhew3.mediamogul.xml.BadlyFormattedXMLException;
import org.joda.time.DateTimeUtils;
import org.joda.time.LocalDate;
import org.junit.Test;

import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static org.fest.assertions.api.Assertions.assertThat;

@SuppressWarnings("ConstantConditions")
public class TiVoDaylightSavingsTest extends DatabaseTest {
  private TiVoLocalProvider tiVoLocalProvider;

  private static Timestamp errorTimestamp = Timestamp.valueOf("2016-11-06 01:01:59.0");
  private static Timestamp correctTimestamp = Timestamp.valueOf("2016-11-06 00:01:59.0");
  private final String programId = "EP0351236842-0362508025";

  @Override
  public void setUp() throws URISyntaxException, SQLException, MissingEnvException {
    super.setUp();
    tiVoLocalProvider = new TiVoLocalProvider(
        "src\\test\\resources\\AtlantaDaylightSavings\\",
        "SeriesList.xml",
        "SeriesDetail.xml");
  }

  @Test
  public void testAtlantaFebruary() throws SQLException, BadlyFormattedXMLException {
    setupSeries();

    LocalDate februaryDate = new LocalDate(2017, 2, 22);

    DateTimeUtils.setCurrentMillisFixed(februaryDate.toDate().getTime());

    TiVoCommunicator tiVoCommunicator = new TiVoCommunicator(connection, tiVoLocalProvider, UpdateMode.FULL);
    tiVoCommunicator.runUpdate();

    TiVoEpisode tiVoEpisode = getSingleAddedEpisode();

    assertThat(tiVoEpisode.captureDate.getValue())
        .isEqualTo(correctTimestamp);
  }

  @Test
  public void testAtlantaApril() throws SQLException, BadlyFormattedXMLException {
    setupSeries();

    LocalDate aprilDate = new LocalDate(2017, 4, 22);

    DateTimeUtils.setCurrentMillisFixed(aprilDate.toDate().getTime());

    TiVoCommunicator tiVoCommunicator = new TiVoCommunicator(connection, tiVoLocalProvider, UpdateMode.FULL);
    tiVoCommunicator.runUpdate();

    TiVoEpisode tiVoEpisode = getSingleAddedEpisode();

    assertThat(tiVoEpisode.captureDate.getValue())
        .isEqualTo(correctTimestamp);
  }

  @Test
  public void testAtlantaDuplicateFebruary() throws SQLException, BadlyFormattedXMLException {
    Series series = setupSeries();
    addTiVoEpisode(series);

    LocalDate februaryDate = new LocalDate(2017, 2, 22);

    DateTimeUtils.setCurrentMillisFixed(februaryDate.toDate().getTime());

    TiVoCommunicator tiVoCommunicator = new TiVoCommunicator(connection, tiVoLocalProvider, UpdateMode.FULL);
    tiVoCommunicator.runUpdate();

    List<TiVoEpisode> matchingEpisodes = findAddedEpisodes();

    assertThat(matchingEpisodes)
        .hasSize(2);

    TiVoEpisode existing = matchingEpisodes.stream()
        .filter(tiVoEpisode -> errorTimestamp.equals(tiVoEpisode.captureDate.getValue()))
        .findFirst()
        .get();

    matchingEpisodes.remove(existing);
    TiVoEpisode addedEpisode = matchingEpisodes.get(0);

    assertThat(addedEpisode.captureDate.getValue())
        .isEqualTo(correctTimestamp);
  }

  @Test
  public void testAtlantaDuplicateApril() throws SQLException, BadlyFormattedXMLException {
    Series series = setupSeries();
    addTiVoEpisode(series);

    LocalDate aprilDate = new LocalDate(2017, 4, 22);

    DateTimeUtils.setCurrentMillisFixed(aprilDate.toDate().getTime());

    TiVoCommunicator tiVoCommunicator = new TiVoCommunicator(connection, tiVoLocalProvider, UpdateMode.FULL);
    tiVoCommunicator.runUpdate();

    List<TiVoEpisode> matchingEpisodes = findAddedEpisodes();

    assertThat(matchingEpisodes)
        .hasSize(2);

    TiVoEpisode existing = matchingEpisodes.stream()
        .filter(tiVoEpisode -> errorTimestamp.equals(tiVoEpisode.captureDate.getValue()))
        .findFirst()
        .get();

    matchingEpisodes.remove(existing);
    TiVoEpisode addedEpisode = matchingEpisodes.get(0);

    assertThat(addedEpisode.captureDate.getValue())
        .isEqualTo(correctTimestamp);
  }

  // utility methods

  private TiVoEpisode getSingleAddedEpisode() throws SQLException {
    List<TiVoEpisode> addedEpisodes = findAddedEpisodes();

    assertThat(addedEpisodes)
        .hasSize(1);

    return addedEpisodes.get(0);
  }

  private Series setupSeries() throws SQLException {
    Series series = new Series();
    series.initializeForInsert();
    series.seriesTitle.changeValue("Atlanta");
    series.tivoSeriesV2ExtId.changeValue("SH0351236842");
    series.commit(connection);

    return series;
  }

  private void addTiVoEpisode(Series series) throws SQLException {
    TiVoEpisode tiVoEpisode = new TiVoEpisode();
    tiVoEpisode.initializeForInsert();

    tiVoEpisode.captureDate.changeValue(errorTimestamp);
    tiVoEpisode.showingStartTime.changeValue(errorTimestamp);
    tiVoEpisode.tivoSeriesV2ExtId.changeValue(series.tivoSeriesV2ExtId.getValue());
    tiVoEpisode.programV2Id.changeValue(programId);
    tiVoEpisode.title.changeValue("The Jacket");
    tiVoEpisode.seriesTitle.changeValue("Atlanta");
    tiVoEpisode.recordingNow.changeValue(false);

    tiVoEpisode.commit(connection);
  }

  private List<TiVoEpisode> findAddedEpisodes() throws SQLException {
    List<TiVoEpisode> addedEpisodes = new ArrayList<>();

    String sql = "SELECT * " +
        "FROM tivo_episode " +
        "WHERE program_v2_id = ? " +
        "AND retired = ? ";
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, programId, 0);

    while (resultSet.next()) {
      TiVoEpisode tiVoEpisode = new TiVoEpisode();
      tiVoEpisode.initializeFromDBObject(resultSet);

      addedEpisodes.add(tiVoEpisode);
    }

    return addedEpisodes;
  }

}
