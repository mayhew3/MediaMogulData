package com.mayhew3.mediamogul.tv.utility;

import com.google.common.collect.Lists;
import com.mayhew3.mediamogul.ArgumentChecker;
import com.mayhew3.mediamogul.db.ConnectionDetails;
import com.mayhew3.mediamogul.model.tv.*;
import com.mayhew3.mediamogul.model.tv.group.TVGroupBallot;
import com.mayhew3.mediamogul.model.tv.group.TVGroupEpisode;
import com.mayhew3.mediamogul.model.tv.group.TVGroupSeries;
import com.mayhew3.mediamogul.model.tv.group.TVGroupVote;
import com.mayhew3.postgresobject.dataobject.DataObject;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;

import java.net.URISyntaxException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

public class SeriesDeleter {

  private static SQLConnection connection;

  private static Logger logger = LogManager.getLogger(SeriesDeleter.class);

  public static void main(String... args) throws URISyntaxException, SQLException {
    ArgumentChecker argumentChecker = new ArgumentChecker(args);
    ConnectionDetails connectionDetails = ConnectionDetails.getConnectionDetails(argumentChecker);

    connection = PostgresConnectionFactory.initiateDBConnect(connectionDetails.getDbUrl());

    runUpdateOnRetired();
  }

  private static void runUpdateOnRetired() throws SQLException {

    logger.info("Beginning full deletion of retired rows... ");

    /*
      - series
      |-- episode_group_rating
      |-- tvdb_series
        |-- tvdb_episode
        |-- tvdb_migration_log
        |-- tvdb_poster
      |-- season
        |-- season_viewing_location
      |-- possible_series_match
      |-- possible_episode_match
      |-- episode
        |-- edge_tivo_episode
          |-- tivo_episode
        |-- episode_rating
      |-- series_genre
      |-- series_viewing_location
      |-- tvdb_migration_error
      |-- tvdb_work_item
      |-- tv_group_series
        |-- tv_group_ballot
          |-- tv_group_vote
      |-- person_series
      |-- tvdb_update_error
     */

    retireRowsWhereReferenceIsRetired(new TVGroupEpisode(), new Episode());
    retireRowsWhereReferenceIsRetired(new PossibleEpisodeMatch(), new TVDBEpisode());
    retireRowsWhereReferenceIsRetired(new PossibleEpisodeMatch(), new TiVoEpisode());
    retireRowsWhereReferenceIsRetired(new TVDBUpdateError(), new Series());

    List<DataObject> tablesToDelete = Lists.newArrayList(
        new EpisodeGroupRating(),

        new TVDBPoster(),
        new TVDBMigrationLog(),
        new TVDBPoster(),
        new TVDBUpdateError(),

        new PossibleSeriesMatch(),
        new PossibleEpisodeMatch(),

//        new TmpRating(),
        new TVGroupEpisode(),
        new TVDBEpisode(),
        new TiVoEpisode(),
        new EpisodeRating(),
        new Episode(),

        new SeasonViewingLocation(),
        new Season(),

        new SeriesGenre(),

        new SeriesViewingLocation(),

        new TVDBMigrationError(),

        new TVDBWorkItem(),

        new TVGroupVote(),
        new TVGroupBallot(),
        new TVGroupSeries(),

        new PersonSeries(),

        new Series(),

        new TVDBSeries()
    );

    for (DataObject dataObject : tablesToDelete) {
      deleteRetiredRowsInTable(dataObject.getTableName());
    }

    logger.info("Full delete complete.");
  }

  private static void retireRowsWhereReferenceIsRetired(DataObject table, DataObject refTable) throws SQLException {

    String fkColumn = refTable.getTableName() + "_id ";
    String tableName = table.getTableName();

    connection.prepareAndExecuteStatementUpdate(
        "UPDATE " + tableName + " " +
            "SET retired = " + tableName + ".id, " +
            "    retired_date = ref.retired_date " +
            "FROM " + refTable.getTableName() + " ref " +
            "WHERE " + tableName + "." + fkColumn + " = ref.id " +
            "AND " + tableName + ".retired = ? " +
            "AND ref.retired <> ? ",
        0, 0
    );
  }

  private static void deleteRetiredRowsInTable(String tableName) throws SQLException {
    logger.info("Deleting from " + tableName + "...");
    DateTime thirtyDaysAgo = new DateTime().minusDays(0);
    Integer deletedRows = connection.prepareAndExecuteStatementUpdate(
        "DELETE FROM " + tableName + " " +
            "WHERE retired <> ? " +
            "AND retired_date < ?",
        0,
        new Timestamp(thirtyDaysAgo.toDate().getTime())
    );
    logger.info("Deleted " + deletedRows + " retired rows from " + tableName);
  }

}
