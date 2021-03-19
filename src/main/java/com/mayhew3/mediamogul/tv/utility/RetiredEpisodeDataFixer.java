package com.mayhew3.mediamogul.tv.utility;

import com.mayhew3.mediamogul.db.DatabaseEnvironments;
import com.mayhew3.mediamogul.model.tv.Episode;
import com.mayhew3.mediamogul.model.tv.EpisodeRating;
import com.mayhew3.mediamogul.model.tv.TVDBEpisode;
import com.mayhew3.mediamogul.model.tv.group.TVGroupEpisode;
import com.mayhew3.mediamogul.tv.exception.ShowFailedException;
import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.db.DatabaseEnvironment;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
import com.mayhew3.postgresobject.exception.MissingEnvException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

public class RetiredEpisodeDataFixer {
  private SQLConnection connection;

  private static Logger logger = LogManager.getLogger(TVDBDuplicateDataFixer.class);


  private RetiredEpisodeDataFixer(SQLConnection connection) {
    this.connection = connection;
  }

  public static void main(String... args) throws URISyntaxException, SQLException, ShowFailedException, MissingEnvException {
    ArgumentChecker argumentChecker = new ArgumentChecker(args);

    DatabaseEnvironment environment = DatabaseEnvironments.getEnvironmentForDBArgument(argumentChecker);
    SQLConnection connection = PostgresConnectionFactory.createConnection(environment);
    RetiredEpisodeDataFixer dataFixer = new RetiredEpisodeDataFixer(connection);
    dataFixer.runUpdate();
  }

  private void runUpdate() throws SQLException, ShowFailedException {
    String sql = "SELECT e.* " +
        "FROM episode e " +
        "WHERE e.retired <> ? " +
        "AND e.id IN (SELECT episode_id FROM episode_rating WHERE retired = ?) ";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, 0, 0);
    while (resultSet.next()) {
      Episode episode = new Episode();
      episode.initializeFromDBObject(resultSet);

      logger.debug("Examining episode: " + episode);

      TVDBEpisode tvdbEpisode = getRetiredTVDBEpisode(episode.tvdbEpisodeId.getValue());

      Episode duplicate = getDuplicate(episode);
      TVDBEpisode dupeTVDBEpisode = duplicate.getTVDBEpisode(connection);

      if (shouldFix(episode, duplicate)) {
        if (shouldUnRetire(tvdbEpisode, dupeTVDBEpisode)) {
          swapEpisodes(episode, tvdbEpisode, duplicate, dupeTVDBEpisode);
        } else {
          moveRatings(episode, duplicate);
        }

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

  private void moveRatings(Episode original, Episode duplicate) throws SQLException {
    List<EpisodeRating> episodeRatings = original.getEpisodeRatings(connection);
    List<TVGroupEpisode> tvGroupEpisodes = original.getTVGroupEpisodes(connection);

    for (EpisodeRating episodeRating : episodeRatings) {
      episodeRating.episodeId.changeValue(duplicate.id.getValue());
      episodeRating.commit(connection);
    }
    for (TVGroupEpisode tvGroupEpisode : tvGroupEpisodes) {
      tvGroupEpisode.episode_id.changeValue(duplicate.id.getValue());
      tvGroupEpisode.commit(connection);
    }
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

  private boolean shouldUnRetire(TVDBEpisode original, TVDBEpisode duplicate) {
    return Objects.equals(original.tvdbEpisodeExtId.getValue(), duplicate.tvdbEpisodeExtId.getValue());
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
        "inner join tvdb_episode te2 on (te1.tvdb_episode_ext_id = te2.tvdb_episode_ext_id" +
        "                               OR (te1.season_number = te2.season_number AND te1.episode_number = te2.episode_number AND te1.tvdb_series_id = te2.tvdb_series_id)) " +
        "inner join episode e2 on e2.tvdb_episode_id = te2.id " +
        "where te1.id <> te2.id " +
        "and te1.retired <> ? " +
        "and te2.retired = ? " +
        "AND e1.id = ? ";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, 0, 0, episode.id.getValue());
    if (resultSet.next()) {
      Episode duplicate = new Episode();
      duplicate.initializeFromDBObject(resultSet);

      if (resultSet.next()) {
        throw new IllegalStateException("Found more than one duplicate for episode: " + episode);
      }
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
