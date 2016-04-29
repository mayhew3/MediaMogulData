package com.mayhew3.gamesutil.tv;

import com.google.common.collect.Ordering;
import com.mayhew3.gamesutil.ArgumentChecker;
import com.mayhew3.gamesutil.dataobject.Episode;
import com.mayhew3.gamesutil.dataobject.Series;
import com.mayhew3.gamesutil.dataobject.TVDBEpisode;
import com.mayhew3.gamesutil.dataobject.TiVoEpisode;
import com.mayhew3.gamesutil.db.PostgresConnectionFactory;
import com.mayhew3.gamesutil.db.SQLConnection;
import com.sun.istack.internal.NotNull;
import org.apache.commons.lang3.ObjectUtils;

import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;

public class TVDBDuplicateDataFixer {
  private SQLConnection connection;

  private final Ordering<Episode> DATEADDED = new Ordering<Episode>() {
    @Override
    public int compare(@NotNull Episode episode1, @NotNull Episode episode2) {
      return Objects.compare(episode1.dateAdded.getValue(), episode2.dateAdded.getValue(),
          (o1, o2) -> ObjectUtils.compare(o1, o2, false));
    }
  };

  private final Ordering<Episode> DATEADDED_REVERSE = new Ordering<Episode>() {
    @Override
    public int compare(@NotNull Episode episode1, @NotNull Episode episode2) {
      return Objects.compare(episode1.dateAdded.getValue(), episode2.dateAdded.getValue(),
          (o1, o2) -> ObjectUtils.compare(o1, o2, true));
    }
  };

  private final Ordering<Episode> WATCHEDDATE = new Ordering<Episode>() {
    @Override
    public int compare(@NotNull Episode episode1, @NotNull Episode episode2) {
      return Objects.compare(episode1.watchedDate.getValue(), episode2.watchedDate.getValue(),
          (o1, o2) -> ObjectUtils.compare(o1, o2, false));
    }
  };

  private final Ordering<Episode> ONTIVO = new Ordering<Episode>() {
    @Override
    public int compare(@NotNull Episode episode1, @NotNull Episode episode2) {
      return Objects.compare(episode1.onTiVo.getValue(), episode2.onTiVo.getValue(),
          (o1, o2) -> ObjectUtils.compare(o1, o2, false));
    }
  };

  private final Ordering<Episode> WATCHED = new Ordering<Episode>() {
    @Override
    public int compare(@NotNull Episode episode1, @NotNull Episode episode2) {
      return Objects.compare(episode1.watched.getValue(), episode2.watched.getValue(),
          (o1, o2) -> ObjectUtils.compare(o1, o2, false));
    }
  };



  public TVDBDuplicateDataFixer(SQLConnection connection) {
    this.connection = connection;
  }

  public static void main(String... args) throws URISyntaxException, SQLException {
    String identifier = new ArgumentChecker(args).getDBIdentifier();

    SQLConnection connection = new PostgresConnectionFactory().createConnection(identifier);
    TVDBDuplicateDataFixer dataFixer = new TVDBDuplicateDataFixer(connection);
    dataFixer.runUpdate();

    new SeriesDenormUpdater(connection).updateFields();
  }

  private void runUpdate() throws SQLException {
    List<ShowFailedException> exceptions = new ArrayList<>();

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(
        "SELECT e.seriesid, e.season, e.season_episode_number\n" +
            "FROM episode e\n" +
            "WHERE retired = ? " +
            "AND season <> ? " +
            "GROUP BY e.seriesid, e.series_title, e.season, e.season_episode_number\n" +
            "HAVING count(1) > ?\n" +
            "ORDER BY e.series_title, e.season, e.season_episode_number",
        0, 0, 1);

    while (resultSet.next()) {
      Integer seriesid = resultSet.getInt("seriesid");
      Integer season = resultSet.getInt("season");
      Integer seasonEpisodeNumber = resultSet.getInt("season_episode_number");

      try {
        resolveDuplicatesForEpisode(seriesid, season, seasonEpisodeNumber);
      } catch (ShowFailedException e) {
        e.printStackTrace();
        exceptions.add(e);
      }
    }

    debug("Finished.");

    debug("Shows failed: " + exceptions.size());
    exceptions.forEach(this::debug);
  }

  private void resolveDuplicatesForEpisode(Integer seriesid, Integer season, Integer seasonEpisodeNumber) throws SQLException, ShowFailedException {
    debug("- SeriesID " + seriesid + ", Season " + season + ", Episode " + seasonEpisodeNumber);

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(
        "SELECT * " +
            "FROM episode " +
            "WHERE seriesid = ? " +
            "AND season = ? " +
            "AND season_episode_number = ? " +
            "AND retired = ? ", seriesid, season, seasonEpisodeNumber, 0);

    List<Episode> olderEpisodes = new ArrayList<>();
    Set<TiVoEpisode> tivoEpisodes = new HashSet<>();

    while (resultSet.next()) {
      Episode episode = new Episode();
      episode.initializeFromDBObject(resultSet);

      olderEpisodes.add(episode);
      tivoEpisodes.addAll(episode.getTiVoEpisodes(connection));
    }

    if (tivoEpisodes.size() > 1) {
      for (TiVoEpisode tivoEpisode : tivoEpisodes) {
        if (unlinkIncorrectEpisodes(tivoEpisode)) {
          tivoEpisodes.remove(tivoEpisode);
        }
      }
    }

    Boolean watched = WATCHED.max(olderEpisodes).watched.getValue();
    Timestamp watchedDate = WATCHEDDATE.max(olderEpisodes).watchedDate.getValue();
    Boolean onTivo = ONTIVO.max(olderEpisodes).onTiVo.getValue();
    Timestamp dateAdded = DATEADDED_REVERSE.min(olderEpisodes).dateAdded.getValue();

    debug("  - " + olderEpisodes.size() + " duplicates.");

    Episode mostRecentEpisode = DATEADDED.max(olderEpisodes);

    if (dateAdded == null) {
      mostRecentEpisode = getTieBreakLastUpdated(olderEpisodes);
    }

    debug("  - Episode " + mostRecentEpisode.id.getValue() + " chosen with DateAdded " + mostRecentEpisode.dateAdded.getValue());

    olderEpisodes.remove(mostRecentEpisode);

    for (Episode episode : olderEpisodes) {
      if (!tivoEpisodes.isEmpty()) {
        unlinkAllTiVoEpisodes(episode);
      }

      episode.retired.changeValue(episode.id.getValue());
      episode.commit(connection);

      TVDBEpisode tvdbEpisode = episode.getTVDBEpisode(connection);
      tvdbEpisode.retired.changeValue(tvdbEpisode.id.getValue());
      tvdbEpisode.commit(connection);
    }

    debug("  - " + olderEpisodes.size() + " episodes and tvdb_episodes removed.");

    mostRecentEpisode.watched.changeValue(watched);
    mostRecentEpisode.watchedDate.changeValue(watchedDate);
    mostRecentEpisode.onTiVo.changeValue(onTivo);
    mostRecentEpisode.dateAdded.changeValue(dateAdded);
    mostRecentEpisode.commit(connection);

    for (TiVoEpisode tivoEpisode : tivoEpisodes) {
      mostRecentEpisode.addToTiVoEpisodes(connection, tivoEpisode);
    }

    TVDBEpisode mostRecentTVDBEpisode = mostRecentEpisode.getTVDBEpisode(connection);
    mostRecentTVDBEpisode.dateAdded.changeValue(dateAdded);
    mostRecentTVDBEpisode.commit(connection);
  }

  private Episode getTieBreakLastUpdated(List<Episode> episodes) throws ShowFailedException, SQLException {

    final Ordering<TVDBEpisode> LASTUPDATED = new Ordering<TVDBEpisode>() {
      @Override
      public int compare(@NotNull TVDBEpisode episode1, @NotNull TVDBEpisode episode2) {
        return Objects.compare(episode1.lastUpdated.getValue(), episode2.lastUpdated.getValue(),
            (o1, o2) -> ObjectUtils.compare(o1, o2, false));
      }
    };

    Map<TVDBEpisode, Episode> tvdbEpisodes = new HashMap<>();
    for (Episode episode : episodes) {
      TVDBEpisode tvdbEpisode = episode.getTVDBEpisode(connection);
      tvdbEpisodes.put(tvdbEpisode, episode);
    }

    TVDBEpisode mostUpdated = LASTUPDATED.max(tvdbEpisodes.keySet());
    if (mostUpdated.lastUpdated.getValue() == null) {
      throw new ShowFailedException("No LastUpdated field on TVDBEpisodes.");
    }

    return tvdbEpisodes.get(mostUpdated);
  }

  private void unlinkAllTiVoEpisodes(Episode episode) throws SQLException {
    connection.prepareAndExecuteStatementUpdate("" +
        "DELETE FROM edge_tivo_episode " +
        "WHERE episode_id = ?", episode.id.getValue());
  }

  private Boolean unlinkIncorrectEpisodes(TiVoEpisode tiVoEpisode) throws SQLException, ShowFailedException {
    List<Episode> episodes = tiVoEpisode.getEpisodes(connection);
    if (episodes.size() > 1) {
      throw new ShowFailedException("Don't know how to handle multi-episode recording yet: " +
          tiVoEpisode.seriesTitle.getValue() + ": " + tiVoEpisode.episodeNumber.getValue() + " " +
          tiVoEpisode.title.getValue());
    }

    Episode episode = episodes.get(0);

    if (betterMatchExists(tiVoEpisode, episode)) {
      connection.prepareAndExecuteStatementUpdate(
          "DELETE FROM edge_tivo_episode " +
              "WHERE tivo_episode_id = ?", tiVoEpisode.id.getValue()
      );
      return true;
    }
    return false;
  }

  private Boolean betterMatchExists(TiVoEpisode tiVoEpisode, Episode currentlyMatched) throws ShowFailedException, SQLException {
    if (tiVoEpisode.title.getValue().equals(currentlyMatched.title.getValue())) {
      return false;
    } else {
      Series series = currentlyMatched.getSeries(connection);
      List<Episode> otherSeriesEpisodes = series.getEpisodes(connection);
      otherSeriesEpisodes.remove(currentlyMatched);

      for (Episode episode : otherSeriesEpisodes) {
        if (tiVoEpisode.title.getValue().equals(episode.title.getValue())) {
          return true;
        }
      }
    }
    return false;
  }



  protected void debug(Object object) {
    System.out.println(object);
  }

}
