package com.mayhew3.gamesutil.tv;

import com.mayhew3.gamesutil.ArgumentChecker;
import com.mayhew3.gamesutil.db.PostgresConnectionFactory;
import com.mayhew3.gamesutil.db.SQLConnection;
import com.mayhew3.gamesutil.model.tv.Episode;
import com.mayhew3.gamesutil.model.tv.Series;
import com.mayhew3.gamesutil.model.tv.TiVoEpisode;

import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class SeriesMerger {
  private Series unmatchedSeries;
  private Series baseSeries;
  private SQLConnection connection;

  SeriesMerger(Series unmatchedSeries, Series baseSeries, SQLConnection connection) {
    this.unmatchedSeries = unmatchedSeries;
    this.baseSeries = baseSeries;
    this.connection = connection;
  }

  public static void main(String... args) throws URISyntaxException, SQLException, ShowFailedException {
    String unmatchedTitle = "HUMANS";
    String baseTitle = "Humans";

    SQLConnection connection = new PostgresConnectionFactory().createConnection(new ArgumentChecker(args).getDBIdentifier());

    Optional<Series> unmatchedSeries = Series.findSeriesFromTitle(unmatchedTitle, connection);
    if (!unmatchedSeries.isPresent()) {
      throw new IllegalStateException("Unable to find existing series with title " + unmatchedTitle);
    }

    Optional<Series> baseSeries = Series.findSeriesFromTitle(baseTitle, connection);
    if (!baseSeries.isPresent()) {
      throw new IllegalStateException("Unable to find existing series with title " + baseTitle);
    }

    SeriesMerger seriesMerger = new SeriesMerger(unmatchedSeries.get(), baseSeries.get(), connection);
    seriesMerger.executeMerge();

    new SeriesDenormUpdater(connection).updateFields();
  }

  void executeMerge() throws SQLException, ShowFailedException {
    List<TiVoEpisode> tiVoEpisodes = unmatchedSeries.getTiVoEpisodes(connection);
    validateNoOtherEpisodes(tiVoEpisodes);

    if (unmatchedSeries.tivoSeriesV2ExtId.getValue() == null || baseSeries.tivoSeriesV2ExtId.getValue() != null) {
      throw new RuntimeException("Currently only supports merging TiVo show into Non-TiVo show.");
    }

    baseSeries.addViewingLocation(connection, "TiVo");

    for (TiVoEpisode tiVoEpisode : tiVoEpisodes) {
      TVDBEpisodeMatcher matcher = new TVDBEpisodeMatcher(connection, tiVoEpisode, baseSeries.id.getValue());
      matcher.matchAndLinkEpisode();
    }

    baseSeries.tivoName.changeValue(unmatchedSeries.seriesTitle.getValue());
    baseSeries.isSuggestion.changeValue(unmatchedSeries.isSuggestion.getValue());
    baseSeries.tivoVersion.changeValue(unmatchedSeries.tivoVersion.getValue());
    baseSeries.tivoSeriesV2ExtId.changeValue(unmatchedSeries.tivoSeriesV2ExtId.getValue());
    baseSeries.tvdbMatchStatus.changeValue(TVDBMatchStatus.MATCH_COMPLETED);

    baseSeries.commit(connection);

    SeriesDeleter seriesDeleter = new SeriesDeleter(unmatchedSeries, connection);
    seriesDeleter.executeDelete();
  }

  private void validateNoOtherEpisodes(List<TiVoEpisode> tiVoEpisodes) throws SQLException {
    Set<Integer> linkedSeries = new HashSet<>();
    for (TiVoEpisode tiVoEpisode : tiVoEpisodes) {
      List<Episode> episodes = tiVoEpisode.getEpisodes(connection);
      for (Episode episode : episodes) {
        Integer seriesId = episode.seriesId.getValue();
        linkedSeries.add(seriesId);
      }
    }
    linkedSeries.remove(baseSeries.id.getValue());
    if (!linkedSeries.isEmpty()) {
      throw new IllegalStateException("TiVoEpisode found linked to another series: " + linkedSeries);
    }
  }
}
