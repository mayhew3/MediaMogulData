package com.mayhew3.mediamogul.tv.utility;

import com.mayhew3.mediamogul.model.tv.Episode;
import com.mayhew3.mediamogul.model.tv.EpisodeRating;
import com.mayhew3.mediamogul.model.tv.TVDBEpisode;
import com.mayhew3.mediamogul.tv.exception.ShowFailedException;
import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;

public class RetiredEpisodeDataFixer {
  private SQLConnection connection;

  private static Logger logger = LogManager.getLogger(TVDBDuplicateDataFixer.class);


  private RetiredEpisodeDataFixer(SQLConnection connection) {
    this.connection = connection;
  }

  public static void main(String... args) throws URISyntaxException, SQLException, ShowFailedException {
    ArgumentChecker argumentChecker = new ArgumentChecker(args);

    SQLConnection connection = PostgresConnectionFactory.createConnection(argumentChecker);
    RetiredEpisodeDataFixer dataFixer = new RetiredEpisodeDataFixer(connection);
    dataFixer.runUpdate();
  }

  private void runUpdate() throws SQLException, ShowFailedException {
    String sql = "select e1.* " +
        "from episode e1 " +
        "inner join tvdb_episode te1 on e1.tvdb_episode_id = te1.id " +
        "inner join tvdb_episode te2 on te1.tvdb_episode_ext_id = te2.tvdb_episode_ext_id " +
        "inner join episode e2 on e2.tvdb_episode_id = te2.id " +
        "where te1.id <> te2.id " +
        "and te1.retired <> ? " +
        "and te2.retired = ? " +
        "and e1.series_title = ? " +
        "order by e1.series_title, e1.season, e1.episode_number";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, 0, 0, "Futurama");
    while (resultSet.next()) {
      Episode episode = new Episode();
      episode.initializeFromDBObject(resultSet);

      logger.debug("Un-retiring episode: " + episode);

      TVDBEpisode tvdbEpisode = getRetiredTVDBEpisode(episode.tvdbEpisodeId.getValue());

      Episode duplicate = getDuplicate(episode);
      TVDBEpisode dupeTVDBEpisode = duplicate.getTVDBEpisode(connection);

      if (shouldFix(episode, duplicate)) {
        swapEpisodes(episode, tvdbEpisode, duplicate, dupeTVDBEpisode);
        logger.debug("Finished swapping!");
      } else {
        logger.debug("Skipping episode.");
      }

    }
  }

  private void swapEpisodes(Episode episode, TVDBEpisode tvdbEpisode, Episode duplicate, TVDBEpisode dupeTVDBEpisode) throws SQLException {
    duplicate.retire();
    duplicate.commit(connection);
    dupeTVDBEpisode.retire();
    dupeTVDBEpisode.commit(connection);

    tvdbEpisode.unRetire();
    tvdbEpisode.commit(connection);
    episode.unRetire();
    episode.commit(connection);
  }

  private boolean shouldFix(Episode original, Episode duplicate) throws SQLException {
    EpisodeRating duplicateRating = duplicate.getMostRecentRating(connection);
    return duplicateRating == null &&
        !hasGroupRating(duplicate) &&
        (original.dateAdded.getValue() == null ||
            original.dateAdded.getValue().before(duplicate.dateAdded.getValue())) &&
        original.season.getValue().equals(duplicate.season.getValue()) &&
        original.episodeNumber.getValue().equals(duplicate.episodeNumber.getValue());
  }

  private boolean hasGroupRating(Episode episode) throws SQLException {
    String sql = "SELECT COUNT(1) as group_eps " +
        "FROM tv_group_episode " +
        "WHERE retired = ? " +
        "AND episode_id = ? ";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, 0, episode.id.getValue());
    if (resultSet.next()) {
      int group_eps = resultSet.getInt("group_eps");
      return group_eps > 0;
    } else {
      throw new RuntimeException("Unknown error executing group rating query.");
    }
  }

  private Episode getDuplicate(Episode episode) throws SQLException {
    String sql = "select e2.* " +
        "from episode e1 " +
        "inner join tvdb_episode te1 on e1.tvdb_episode_id = te1.id " +
        "inner join tvdb_episode te2 on te1.tvdb_episode_ext_id = te2.tvdb_episode_ext_id " +
        "inner join episode e2 on e2.tvdb_episode_id = te2.id " +
        "where te1.id <> te2.id " +
        "and te1.retired <> ? " +
        "and te2.retired = ? " +
        "AND e1.id = ? ";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, 0, 0, episode.id.getValue());
    if (resultSet.next()) {
      Episode duplicate = new Episode();
      duplicate.initializeFromDBObject(resultSet);
      return duplicate;
    } else {
      throw new IllegalStateException("No duplicate found for episode " + episode);
    }
  }

  private TVDBEpisode getRetiredTVDBEpisode(int tvdb_episode_id) throws SQLException {
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(
        "SELECT * " +
            "FROM tvdb_episode " +
            "WHERE id = ? " +
            "AND retired <> ?",
        tvdb_episode_id,
        0
    );

    if (!resultSet.next()) {
      throw new IllegalStateException("No tvdb_episode found with id of " + tvdb_episode_id);
    }
    TVDBEpisode tvdbEpisode = new TVDBEpisode();
    tvdbEpisode.initializeFromDBObject(resultSet);
    return tvdbEpisode;
  }

}
