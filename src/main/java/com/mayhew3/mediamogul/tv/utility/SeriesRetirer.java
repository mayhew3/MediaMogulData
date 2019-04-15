package com.mayhew3.mediamogul.tv.utility;

import com.mayhew3.mediamogul.db.ConnectionDetails;
import com.mayhew3.mediamogul.model.tv.Series;
import com.mayhew3.mediamogul.ArgumentChecker;
import com.mayhew3.mediamogul.tv.TVDBMatchStatus;
import com.mayhew3.mediamogul.tv.helper.UpdateMode;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

public class SeriesRetirer {

  private Series series;
  private SQLConnection connection;

  private static Logger logger = LogManager.getLogger(SeriesRetirer.class);

  public SeriesRetirer(Series series, SQLConnection connection) {
    this.series = series;
    this.connection = connection;
  }

  public static void main(String... args) throws URISyntaxException, SQLException {
    String seriesTitle = "The Good Place";

    ArgumentChecker argumentChecker = new ArgumentChecker(args);
    UpdateMode updateMode = UpdateMode.getUpdateModeOrDefault(argumentChecker, UpdateMode.SINGLE);
    ConnectionDetails connectionDetails = ConnectionDetails.getConnectionDetails(argumentChecker);

    SQLConnection connection = PostgresConnectionFactory.initiateDBConnect(connectionDetails.getDbUrl());

    if (UpdateMode.SINGLE.equals(updateMode)) {
      Optional<Series> series = Series.findSeriesFromTitle(seriesTitle, connection);

      if (series.isPresent()) {
        SeriesRetirer seriesRetirer = new SeriesRetirer(series.get(), connection);
        seriesRetirer.executeDelete();
      } else {
        throw new RuntimeException("Unable to find series with title: " + seriesTitle);
      }
    } else {
      runUpdateSuggestions(connection);
    }
  }

  private static void runUpdateSuggestions(SQLConnection connection) throws SQLException {
    String sql = "select * " +
        "from series " +
        "where suggestion = ? " +
        "and tvdb_match_status <> ? " +
        "and tvdb_match_status <> ? " +
        "and retired = ? " +
        "order by title";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql,
        true,
        TVDBMatchStatus.MATCH_CONFIRMED,
        TVDBMatchStatus.MATCH_COMPLETED,
        0);

    while (resultSet.next()) {
      Series series = new Series();
      series.initializeFromDBObject(resultSet);

      SeriesRetirer seriesRetirer = new SeriesRetirer(series, connection);
      seriesRetirer.executeDelete();
    }
  }

  public void executeDelete() throws SQLException {

    logger.info("Beginning full retiring of series: " + series.seriesTitle.getValue());

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

    retireSeries();
    retireAllTVDBSeriesRows();

    retireRowsWithFKToSeries("episode_group_rating");
    retireRowsWithFKToSeries("possible_series_match");
    retireRowsWithFKToSeries("series_genre");
    retireRowsWithFKToSeries("series_viewing_location");
    retireRowsWithFKToSeries("tvdb_migration_error");
    retireRowsWithFKToSeries("tvdb_work_item");
    retireRowsWithFKToSeries("tvdb_update_error");
    retireRowsWithFKToSeries("person_series");

    retireSeasons();
    retireEpisodes();

    logger.info("Full retire complete.");
  }

  private void retireSeries() throws SQLException {
    Integer updatedRows = connection.prepareAndExecuteStatementUpdate(
        "UPDATE series " +
            "SET retired = id," +
            "    retired_date = now() " +
            "WHERE id = ?",
        series.id.getValue());
    if (updatedRows != 1) {
      throw new RuntimeException("Expected exactly one row updated.");
    }
    logger.info("1 series retired with ID " + series.id.getValue());
  }

  private void retireTVDBSeries() throws SQLException {
    Integer updatedRows = connection.prepareAndExecuteStatementUpdate(
        "UPDATE tvdb_series " +
            "SET retired = id, " +
            "    retired_date = now() " +
            "WHERE id = ?", series.tvdbSeriesId.getValue());
    if (updatedRows != 1) {
      throw new RuntimeException("Expected exactly one row updated.");
    }
    debug("1 tvdb_series retired with ID " + series.tvdbSeriesId.getValue());
  }

  private void retireEpisodes() throws SQLException {
    retireRowsWithFKToSeries("episode");
    retireRowsWhereReferencedRowIsRetired("episode_rating", "episode");
    retireRowsWhereReferencedRowIsRetired("possible_episode_match", "tvdb_episode");
    retireRowsWhereReferencedRowIsRetired("possible_episode_match", "tivo_episode");
    retireRowsWhereReferencedRowIsRetired("tv_group_episode", "episode");
    retireTivoEpisodes();
    deleteTiVoEdgeRows();
  }

  private void deleteTiVoEdgeRows() throws SQLException {
    Integer deletedRows = connection.prepareAndExecuteStatementUpdate(
        "DELETE FROM edge_tivo_episode ete " +
            "USING episode e " +
            "WHERE ete.episode_id = e.id " +
            "AND e.retired <> ? ",
        0);
    debug(deletedRows + " rows deleted from edge_tivo_episode related to retired episodes.");
  }

  private void retireTivoEpisodes() throws SQLException {
    Integer retiredRows = connection.prepareAndExecuteStatementUpdate(
        "UPDATE tivo_episode te " +
            "SET retired = te.id," +
            "    retired_date = now() " +
            "FROM edge_tivo_episode ete " +
            "INNER JOIN episode e " +
            "  ON ete.episode_id = e.id " +
            "WHERE ete.tivo_episode_id = te.id " +
            "AND e.retired <> ? ",
        0
    );
    debug(retiredRows + " rows retired from tivo_episode related to deleted edge rows.");
  }

  private void retireSeasons() throws SQLException {
    retireRowsWithFKToSeries("season");
    retireRowsWhereReferencedRowIsRetired("season_viewing_location", "season");
  }

  private void retireAllTVDBSeriesRows() throws SQLException {
    Integer tvdbSeriesId = series.tvdbSeriesId.getValue();
    if (tvdbSeriesId != null) {
      retireTVDBSeries();

      retireRowsWhereReferencedRowIsRetired("tvdb_migration_log", "tvdb_series");
      retireRowsWhereReferencedRowIsRetired("tvdb_poster", "tvdb_series");
      retireRowsWhereReferencedRowIsRetired("tvdb_episode", "tvdb_series");
    }
  }



  private void retireRowsWhereReferencedRowIsRetired(String tableName, String referencedTable) throws SQLException {
    String sql =
        "UPDATE " + tableName + " tn " +
            "SET retired = tn.id, " +
            "    retired_date = rt.retired_date " +
            "FROM " + referencedTable + " rt " +
            "WHERE tn." + referencedTable + "_id = rt.id " +
            "AND rt.retired <> ? ";
    Integer retiredRows = connection.prepareAndExecuteStatementUpdate(sql, 0);
    debug("Retired " + retiredRows + " rows from table '" + tableName + "' referencing retired rows in table '" + referencedTable + "'");
  }

  private void retireRowsWithFKToSeries(String tableName) throws SQLException {
    Integer series_id = series.id.getValue();
    Integer rowsRetired = retireRowsFromTableMatchingColumn(tableName, "series_id", series_id);

    debug("Retired " + rowsRetired + " rows from table " + tableName + " with series_id " + series_id);
  }

  private Integer retireRowsFromTableMatchingColumn(String tableName, String columnName, Integer id) throws SQLException {
    String sql =
        "UPDATE " + tableName + " " +
            "SET retired = id," +
            "    retired_date = now() " +
            "WHERE " + columnName + " = ? ";
    return connection.prepareAndExecuteStatementUpdate(sql, id);
  }


  private void debug(Object message) {
    logger.debug(message);
  }

}
