package com.mayhew3.mediamogul.tv.utility;

import com.mayhew3.mediamogul.db.DatabaseEnvironments;
import com.mayhew3.mediamogul.model.Person;
import com.mayhew3.mediamogul.model.tv.Series;
import com.mayhew3.mediamogul.model.tv.group.*;
import com.mayhew3.mediamogul.tv.TVDBMatchStatus;
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
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class TVGroupDataImporter {
  private SQLConnection connection;

  private Map<BallotInfo, TVGroupBallot> ballotCache;

  private static Logger logger = LogManager.getLogger(TVGroupDataImporter.class);

  private TVGroupDataImporter(SQLConnection connection) {
    this.connection = connection;
    this.ballotCache = new HashMap<>();
  }

  public static void main(String... args) throws URISyntaxException, SQLException, MissingEnvException {
    ArgumentChecker argumentChecker = new ArgumentChecker(args);

    DatabaseEnvironment environment = DatabaseEnvironments.getEnvironmentForDBArgument(argumentChecker);
    SQLConnection connection = PostgresConnectionFactory.createConnection(environment);
    TVGroupDataImporter tvGroupDataImporter = new TVGroupDataImporter(connection);
    tvGroupDataImporter.runUpdate();
  }

  private void runUpdate() throws SQLException {
    String sql = "SELECT * " +
        "FROM tv_group_vote_import " +
        "WHERE imported = ? ";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, false);
    while (resultSet.next()) {
      TVGroupVoteImport tvGroupVoteImport = new TVGroupVoteImport();
      tvGroupVoteImport.initializeFromDBObject(resultSet);

      String title = tvGroupVoteImport.show.getValue();
      Timestamp voteDate = tvGroupVoteImport.vote_date.getValue();

      Optional<Series> optionalSeries = findSeriesFromTitle(title, connection);
      if (optionalSeries.isPresent()) {
        Series series = optionalSeries.get();
        Person person = getPerson(tvGroupVoteImport.email.getValue());
        TVGroup group = getGroup(null);
        Integer voteValue = tvGroupVoteImport.vote.getValue();

        TVGroupBallot ballot = getOrCreateBallot(group.id.getValue(), voteDate, series);
        submitVote(ballot, voteValue, person);

        tvGroupVoteImport.imported.changeValue(true);
        tvGroupVoteImport.commit(connection);
      } else {
        logger.warn("No series found with title: '" + title + "'");
      }
    }
  }

  private Optional<Series> findSeriesFromTitle(String seriesTitle, SQLConnection connection) throws SQLException {
    String sql = "SELECT * " +
        "FROM series " +
        "WHERE title = ? " +
        "AND retired = ? " +
        "AND tvdb_match_status = ? ";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, seriesTitle, 0, TVDBMatchStatus.MATCH_COMPLETED);

    if (resultSet.next()) {
      Series series = new Series();
      series.initializeFromDBObject(resultSet);
      return Optional.of(series);
    } else {
      return Optional.empty();
    }
  }

  private Person getPerson(String email) throws SQLException {
    String sql = "SELECT * " +
        "FROM person " +
        "WHERE email = ? ";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, email);
    if (resultSet.next()) {
      Person person = new Person();
      person.initializeFromDBObject(resultSet);
      return person;
    } else {
      throw new RuntimeException("No person found with email " + email);
    }
  }

  private TVGroup getGroup(@SuppressWarnings("SameParameterValue") String name) throws SQLException {
    String sql = "SELECT * " +
        "FROM tv_group " +
        "WHERE name = ? ";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, name);
    if (resultSet.next()) {
      TVGroup tvGroup = new TVGroup();
      tvGroup.initializeFromDBObject(resultSet);
      return tvGroup;
    } else {
      throw new RuntimeException("No group found with name: " + name);
    }
  }

  private TVGroupBallot getOrCreateBallot(Integer tv_group_id, Timestamp voteDate, Series series) throws SQLException {
    Optional<TVGroupBallot> ballotFromCache = findBallotFromCache(series, new Date(voteDate.getTime()));
    if (ballotFromCache.isPresent()) {
      return ballotFromCache.get();
    } else {
      TVGroupSeries tvGroupSeries = getOrCreateTVGroupSeries(series, tv_group_id);

      String sql = "SELECT tgb.* " +
          "FROM tv_group_ballot tgb " +
          "WHERE tgb.tv_group_series_id = ? " +
          "AND tgb.retired = ? " +
          "AND tgb.voting_open = ? " +
          "ORDER BY id DESC ";

      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, tvGroupSeries.id.getValue(), 0, voteDate);
      TVGroupBallot tvGroupBallot = new TVGroupBallot();
      if (resultSet.next()) {
        tvGroupBallot.initializeFromDBObject(resultSet);
      } else {
        tvGroupBallot.initializeForInsert();
        tvGroupBallot.voting_open.changeValue(voteDate);
        tvGroupBallot.reason.changeValue("Start Watching");

        tvGroupBallot.tv_group_series.changeValue(tvGroupSeries.id.getValue());
        tvGroupBallot.commit(connection);
      }

      ballotCache.put(new BallotInfo(series.id.getValue(), new Date(voteDate.getTime())), tvGroupBallot);
      return tvGroupBallot;
    }
  }

  private TVGroupSeries getOrCreateTVGroupSeries(Series series, Integer tv_group_id) throws SQLException {
    String sql = "SELECT tgs.* " +
        "FROM tv_group_series tgs " +
        "WHERE tv_group_id = ? " +
        "AND series_id = ? " +
        "AND retired = ? ";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, tv_group_id, series.id.getValue(), 0);
    TVGroupSeries tvGroupSeries = new TVGroupSeries();
    if (resultSet.next()) {
      tvGroupSeries.initializeFromDBObject(resultSet);
    } else {
      tvGroupSeries.initializeForInsert();
      tvGroupSeries.tv_group_id.changeValue(tv_group_id);
      tvGroupSeries.series_id.changeValue(series.id.getValue());
      tvGroupSeries.commit(connection);
    }
    return tvGroupSeries;
  }

  private Optional<TVGroupBallot> findBallotFromCache(Series series, Date voteDate) {
    TVGroupBallot tvGroupBallot = ballotCache.get(new BallotInfo(series.id.getValue(), voteDate));
    if (tvGroupBallot == null) {
      return Optional.empty();
    } else {
      return Optional.of(tvGroupBallot);
    }
  }

  private void submitVote(TVGroupBallot ballot, Integer vote, Person person) throws SQLException {
    TVGroupVote tvGroupVote = new TVGroupVote();
    tvGroupVote.initializeForInsert();

    tvGroupVote.person_id.changeValue(person.id.getValue());
    tvGroupVote.ballot_id.changeValue(ballot.id.getValue());
    tvGroupVote.vote_value.changeValue(vote);

    tvGroupVote.commit(connection);
  }

  private void debug(Object message) {
    logger.debug(message);
  }

  private class BallotInfo {
    public Integer series_id;
    Date voteDate;

    BallotInfo(Integer series_id, Date voteDate) {
      this.series_id = series_id;
      this.voteDate = voteDate;
    }
  }

}
