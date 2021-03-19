package com.mayhew3.mediamogul.tv.utility;

import com.google.common.collect.Lists;
import com.mayhew3.mediamogul.db.ConnectionDetails;
import com.mayhew3.mediamogul.model.MediaMogulSchema;
import com.mayhew3.mediamogul.model.tv.*;
import com.mayhew3.mediamogul.model.tv.group.TVGroupBallot;
import com.mayhew3.mediamogul.model.tv.group.TVGroupEpisode;
import com.mayhew3.mediamogul.model.tv.group.TVGroupSeries;
import com.mayhew3.mediamogul.model.tv.group.TVGroupVote;
import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.dataobject.DataObject;
import com.mayhew3.postgresobject.dataobject.RetireableDataObject;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;

import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

public class SeriesDeleter {

  private static SQLConnection connection;

  @SuppressWarnings("FieldCanBeLocal")
  private static Integer dayThreshold = 30;

  private static Logger logger = LogManager.getLogger(SeriesDeleter.class);

  public static void main(String... args) throws URISyntaxException, SQLException {
    ArgumentChecker argumentChecker = new ArgumentChecker(args);
    ConnectionDetails connectionDetails = ConnectionDetails.getConnectionDetails(argumentChecker);

    connection = PostgresConnectionFactory.initiateDBConnect(connectionDetails.getDbUrl());

    printAllRetiredCounts();
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

    retireRowsWhereReferenceIsRetired(new PossibleEpisodeMatch(), new TVDBEpisode());
    retireRowsWhereReferenceIsRetired(new TVDBMigrationLog(), new TVDBEpisode());
    retireRowsWhereReferenceIsRetired(new PossibleEpisodeMatch(), new TiVoEpisode());
    retireRowsWhereReferenceIsRetired(new TVDBUpdateError(), new Series());
    retireRowsWhereReferenceIsRetired(new Episode(), new TVDBEpisode());
    retireRowsWhereReferenceIsRetired(new TVGroupEpisode(), new Episode());

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
        new TiVoEpisode(),
        new EpisodeRating(),
        new Episode(),
        new TVDBEpisode(),

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

  private static void nullOutReferencesToRetiredRows(DataObject table, DataObject refTable) throws SQLException {
    String fkColumn = refTable.getTableName() + "_id";
    String tableName = table.getTableName();
    Integer affectedCount = connection.prepareAndExecuteStatementUpdate(
        "UPDATE " + tableName + " orig " +
            "SET " + fkColumn + " = NULL " +
            "FROM " + refTable.getTableName() + " ref " +
            "WHERE orig." + fkColumn + " = ref.id " +
            "AND orig.retired = ? " +
            "AND ref.retired <> ? ",
        0, 0
    );
    logger.info("Updated " + affectedCount + " rows, nulling " + fkColumn + " FK to retired row.");
  }

  private static void printAllRetiredCounts() throws SQLException {
    logger.info("Retired counts: ");

    List<DataObject> allTables = MediaMogulSchema.schema.getAllTables();
    for (DataObject table : allTables) {
      if (table instanceof RetireableDataObject) {
        ResultSet resultSet = connection.prepareAndExecuteStatementFetch(
            "SELECT COUNT(1) AS retired_count " +
                "FROM " + table.getTableName() + " " +
                "WHERE retired <> 0 ");
        if (resultSet.next()) {
          int retired_count = resultSet.getInt("retired_count");
          logger.info("- " + table.getTableName() + ": " + retired_count);
        }
      }
    }
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
    DateTime thirtyDaysAgo = new DateTime().minusDays(dayThreshold);
    Integer deletedRows = connection.prepareAndExecuteStatementUpdate(
        "DELETE FROM " + tableName + " " +
            "WHERE retired <> ? " +
            "AND (retired_date < ? " +
            "    OR retired_date IS NULL) ",
        0,
        new Timestamp(thirtyDaysAgo.toDate().getTime())
    );
    logger.info("Deleted " + deletedRows + " retired rows from " + tableName);
  }

}
