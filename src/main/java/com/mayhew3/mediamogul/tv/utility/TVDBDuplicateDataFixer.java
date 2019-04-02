package com.mayhew3.mediamogul.tv.utility;

import com.google.common.collect.Ordering;
import com.mayhew3.mediamogul.scheduler.TaskScheduleRunner;
import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
import com.mayhew3.mediamogul.model.tv.Episode;
import com.mayhew3.mediamogul.model.tv.Series;
import com.mayhew3.mediamogul.model.tv.TVDBEpisode;
import com.mayhew3.mediamogul.model.tv.TiVoEpisode;
import com.mayhew3.mediamogul.tv.SeriesDenormUpdater;
import com.mayhew3.mediamogul.tv.exception.ShowFailedException;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;

public class TVDBDuplicateDataFixer {
  private SQLConnection connection;

  private static Logger logger = LogManager.getLogger(TVDBDuplicateDataFixer.class);

  private final Ordering<Episode> DATEADDED = new Ordering<Episode>() {
    @Override
    public int compare(@Nullable Episode episode1, @Nullable Episode episode2) {
      if (episode1 == null || episode2 == null) {
        throw new IllegalStateException("Cannot order null episodes.");
      }
      return Objects.compare(episode1.dateAdded.getValue(), episode2.dateAdded.getValue(),
          (o1, o2) -> ObjectUtils.compare(o1, o2, false));
    }
  };

  private final Ordering<Episode> DATEADDED_REVERSE = new Ordering<Episode>() {
    @Override
    public int compare(@Nullable Episode episode1, @Nullable Episode episode2) {
      if (episode1 == null || episode2 == null) {
        throw new IllegalStateException("Cannot order null episodes.");
      }
      return Objects.compare(episode1.dateAdded.getValue(), episode2.dateAdded.getValue(),
          (o1, o2) -> ObjectUtils.compare(o1, o2, true));
    }
  };

  private final Ordering<Episode> WATCHEDDATE = new Ordering<Episode>() {
    @Override
    public int compare(@Nullable Episode episode1, @Nullable Episode episode2) {
      if (episode1 == null || episode2 == null) {
        throw new IllegalStateException("Cannot order null episodes.");
      }
      return Objects.compare(episode1.watchedDate.getValue(), episode2.watchedDate.getValue(),
          (o1, o2) -> ObjectUtils.compare(o1, o2, false));
    }
  };

  private final Ordering<Episode> ONTIVO = new Ordering<Episode>() {
    @Override
    public int compare(@Nullable Episode episode1, @Nullable Episode episode2) {
      if (episode1 == null || episode2 == null) {
        throw new IllegalStateException("Cannot order null episodes.");
      }
      return Objects.compare(episode1.onTiVo.getValue(), episode2.onTiVo.getValue(),
          (o1, o2) -> ObjectUtils.compare(o1, o2, false));
    }
  };

  private final Ordering<Episode> WATCHED = new Ordering<Episode>() {
    @Override
    public int compare(@Nullable Episode episode1, @Nullable Episode episode2) {
      if (episode1 == null || episode2 == null) {
        throw new IllegalStateException("Cannot order null episodes.");
      }
      return Objects.compare(episode1.watched.getValue(), episode2.watched.getValue(),
          (o1, o2) -> ObjectUtils.compare(o1, o2, false));
    }
  };



  private TVDBDuplicateDataFixer(SQLConnection connection) {
    this.connection = connection;
  }

  public static void main(String... args) throws URISyntaxException, SQLException {
    ArgumentChecker argumentChecker = new ArgumentChecker(args);

    SQLConnection connection = PostgresConnectionFactory.createConnection(argumentChecker);
    TVDBDuplicateDataFixer dataFixer = new TVDBDuplicateDataFixer(connection);
    dataFixer.runUpdate();

    new SeriesDenormUpdater(connection).runUpdate();
  }

  private void runUpdate() throws SQLException {
    List<ShowFailedException> exceptions = new ArrayList<>();

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(
        "SELECT e.series_id, e.season, e.episode_number\n" +
            "FROM episode e\n" +
            "WHERE retired = ? " +
            "AND season <> ? " +
            "GROUP BY e.series_id, e.series_title, e.season, e.episode_number\n" +
            "HAVING count(1) > ?\n" +
            "ORDER BY e.series_title, e.season, e.episode_number",
        0, 0, 1);

    while (resultSet.next()) {
      Integer seriesid = resultSet.getInt("series_id");
      Integer season = resultSet.getInt("season");
      Integer seasonEpisodeNumber = resultSet.getInt("episode_number");

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
            "WHERE series_id = ? " +
            "AND season = ? " +
            "AND episode_number = ? " +
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

      episode.retire();
      episode.commit(connection);

      TVDBEpisode tvdbEpisode = episode.getTVDBEpisode(connection);
      tvdbEpisode.retire();
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
      public int compare(@Nullable TVDBEpisode episode1, @Nullable TVDBEpisode episode2) {
        if (episode1 == null || episode2 == null) {
          throw new IllegalStateException("Cannot order null episodes.");
        }
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



  private void debug(Object message) {
    logger.debug(message);
  }

}
