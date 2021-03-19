package com.mayhew3.mediamogul.tv;

import com.mayhew3.mediamogul.DatabaseTest;
import com.mayhew3.mediamogul.dataobject.TiVoLocalProvider;
import com.mayhew3.mediamogul.model.tv.Series;
import com.mayhew3.mediamogul.model.tv.TVDBSeries;
import com.mayhew3.mediamogul.tv.helper.UpdateMode;
import com.mayhew3.mediamogul.xml.BadlyFormattedXMLException;
import com.mayhew3.postgresobject.exception.MissingEnvException;
import org.junit.Test;

import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.fest.assertions.api.Assertions.assertThat;

public class TiVoMatchExistingTest extends DatabaseTest {
  private TiVoLocalProvider tiVoLocalProvider;

  @Override
  public void setUp() throws URISyntaxException, SQLException, MissingEnvException {
    super.setUp();
    tiVoLocalProvider = new TiVoLocalProvider(
        "src\\test\\resources\\AtlantaDaylightSavings\\",
        "SeriesListRecordingNow.xml",
        "SeriesDetail.xml");
  }

  @Test
  public void testFirstTiVoEpisodeFoundForExistingSeries() throws SQLException, BadlyFormattedXMLException {
    setupSeries();

    String seriesName = "Atlanta";
    List<Series> originalSeriesesWithName = getSeriesWithName(seriesName);
    assertThat(originalSeriesesWithName)
        .as("SANITY: Only one series should exist with name initially.")
        .hasSize(1);

    TiVoCommunicator tiVoCommunicator = new TiVoCommunicator(connection, tiVoLocalProvider, UpdateMode.FULL);
    tiVoCommunicator.runUpdate();

    TiVoLocalProvider notRecording = new TiVoLocalProvider(
        "src\\test\\resources\\AtlantaDaylightSavings\\",
        "SeriesList.xml",
        "SeriesDetail.xml");

    tiVoCommunicator = new TiVoCommunicator(connection, notRecording, UpdateMode.FULL);
    tiVoCommunicator.runUpdate();

    List<Series> afterUpdate = getSeriesWithName(seriesName);
    assertThat(afterUpdate)
        .as("Expect no new series to be added because one already exists with name.")
        .hasSize(1);
  }


  private void setupSeries() throws SQLException {
    int tvdbSeriesExtId = 234123;

    TVDBSeries tvdbSeries = new TVDBSeries();
    tvdbSeries.initializeForInsert();
    tvdbSeries.tvdbSeriesExtId.changeValue(tvdbSeriesExtId);
    tvdbSeries.commit(connection);

    Series series = new Series();
    series.initializeForInsert();
    series.seriesTitle.changeValue("Atlanta");
    series.addedBy.changeValue("Manual");
    series.tvdbMatchStatus.changeValue("Match Completed");
    series.tvdbSeriesId.changeValue(tvdbSeries.id.getValue());
    series.tvdbSeriesExtId.changeValue(tvdbSeriesExtId);
    series.tivoVersion.changeValue(1);
    series.commit(connection);
  }

  private List<Series> getSeriesWithName(String seriesName) throws SQLException {
    List<Series> serieses = new ArrayList<>();

    String sql = "SELECT * " +
        "FROM series " +
        "WHERE title = ? " +
        "AND retired = ? ";
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, seriesName, 0);

    while (resultSet.next()) {
      Series series = new Series();
      series.initializeFromDBObject(resultSet);

      serieses.add(series);
    }

    return serieses;
  }

}
