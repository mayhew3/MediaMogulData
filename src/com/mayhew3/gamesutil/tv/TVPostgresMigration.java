package com.mayhew3.gamesutil.tv;

import com.google.common.collect.Lists;
import com.mayhew3.gamesutil.games.MongoConnection;
import com.mayhew3.gamesutil.games.PostgresConnection;
import com.mayhew3.gamesutil.mediaobject.*;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.bson.types.ObjectId;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class TVPostgresMigration {
  private static MongoConnection mongoConnection;
  private static PostgresConnection postgresConnection;

  public static void main(String[] args) throws SQLException {
    List<String> argList = Lists.newArrayList(args);
    Boolean devMode = argList.contains("dev");

    postgresConnection = new PostgresConnection();
    mongoConnection = new MongoConnection("tv");

    TVPostgresMigration tvPostgresMigration = new TVPostgresMigration();

    if (devMode) {
      tvPostgresMigration.truncatePostgresTables();
    }

    tvPostgresMigration.updatePostgresDatabase();
  }

  private void truncatePostgresTables() throws SQLException {
    postgresConnection.executeUpdate("TRUNCATE TABLE series CASCADE");
  }

  public void updatePostgresDatabase() throws SQLException {
    DBObject dbObject = new BasicDBObject()
        .append("IsEpisodic", true)
//        .append("IsSuggestion", new BasicDBObject("$ne", true))

        // only include series that were added from TiVo. Manually added shows have no useful data in them at this point anyway,
        // and it'll be difficult to check if they already exist in Postgres.
        .append("TiVoSeriesId", new BasicDBObject("$ne", null))
        ;

    DBCursor dbCursor = mongoConnection.getCollection("series").find(dbObject);

    int totalRows = dbCursor.count();
    debug(totalRows + " series found to copy. Starting.");

    int i = 0;

    while (dbCursor.hasNext()) {
      i++;
      DBObject seriesMongoObject = dbCursor.next();

      updateSingleSeries(seriesMongoObject);

      debug(i + " out of " + totalRows + " processed.");
      debug("");
    }

    debug("Update complete!");

  }


  private void updateSingleSeries(DBObject seriesMongoObject) throws SQLException {
    SeriesMongo seriesMongo = new SeriesMongo();
    seriesMongo.initializeFromDBObject(seriesMongoObject);

    SeriesPostgres seriesPostgres = getOrCreateSeriesPostgres(seriesMongo.tivoSeriesId.getValue());

    String title = seriesMongo.seriesTitle.getValue();

    if (seriesPostgres.id.getValue() != null) {
      debug(title + ": Updating");
    } else {
      debug(title + ": Inserting");
    }

    copyAllSeriesFields(seriesMongo, seriesPostgres);
    seriesPostgres.commit(postgresConnection);

    Integer seriesId = seriesPostgres.id.getValue();
    if (seriesId == null) {
      throw new RuntimeException("No ID populated on series postgres object after insert or update.");
    }

    if (seriesMongo.tvdbId.getValue() != null) {
      debug("   (also copying tvdb info...)");

      TVDBSeriesPostgres tvdbSeriesPostgres = getOrCreateTVDBSeriesPostgres(seriesMongo.tvdbId.getValue());

      tvdbSeriesPostgres.seriesId.changeValue(seriesId);

      copyAllTVDBSeriesFields(seriesMongo, tvdbSeriesPostgres);
      tvdbSeriesPostgres.commit(postgresConnection);
    }

    updateEpisodes(seriesMongo, seriesId);
  }

  private void updateEpisodes(SeriesMongo seriesMongo, Integer seriesId) throws SQLException {
    ObjectId mongoId = seriesMongo._id.getValue();

    BasicDBObject episodeQuery = new BasicDBObject()
        .append("SeriesId", mongoId)
        .append("TiVoProgramId", new BasicDBObject("$ne", null))
        .append("MatchingStump", new BasicDBObject("$ne", true))
        ;

    DBCursor cursor = mongoConnection.getCollection("episodes").find(episodeQuery);

    debug(" - Updating " + cursor.count() + " episodes.");

    while (cursor.hasNext()) {
      DBObject episodeDBObj = cursor.next();

      EpisodeMongo episodeMongo = new EpisodeMongo();
      episodeMongo.initializeFromDBObject(episodeDBObj);

      EpisodePostgres episodePostgres = getOrCreateEpisodePostgres(episodeMongo.tivoProgramId.getValue());

      Integer tvdbEpisodeId = episodeMongo.tvdbEpisodeId.getValue();
      String tvdbInfo = (tvdbEpisodeId == null) ? " (NO TVDB!)" : "";

      if (episodePostgres.id.getValue() == null) {
        debug("    * " + episodeMongo + " (INSERT)" + tvdbInfo);
      } else {
        debug("    * " + episodeMongo + " (UPDATE)" + tvdbInfo);
      }

      episodePostgres.seriesId.changeValue(seriesId);
      copyAllEpisodeFields(episodeMongo, episodePostgres);
      episodePostgres.commit(postgresConnection);

      Integer episodeId = episodePostgres.id.getValue();
      if (episodeId == null) {
        throw new RuntimeException("No ID populated on episode postgres object after insert or update.");
      }

      TiVoEpisodePostgres tiVoEpisodePostgres = getOrCreateTiVoEpisodePostgres(episodeMongo.tivoProgramId.getValue());

      tiVoEpisodePostgres.episodeId.changeValue(episodeId);
      copyAllTiVoEpisodeFields(episodeMongo, tiVoEpisodePostgres);
      tiVoEpisodePostgres.commit(postgresConnection);

      if (tvdbEpisodeId != null) {
        TVDBEpisodePostgres tvdbEpisodePostgres = getOrCreateTVDBEpisodePostgres(tvdbEpisodeId);

        tvdbEpisodePostgres.episodeId.changeValue(episodeId);
        copyAllTVDBEpisodeFields(episodeMongo, tvdbEpisodePostgres);
        tvdbEpisodePostgres.commit(postgresConnection);
      }
    }
  }

  private void copyAllEpisodeFields(EpisodeMongo episodeMongo, EpisodePostgres episodePostgres) {
    episodePostgres.watchedDate.changeValue(episodeMongo.watchedDate.getValue());
    episodePostgres.season.changeValue(episodeMongo.tvdbSeason.getValue());
    episodePostgres.seasonEpisodeNumber.changeValue(episodeMongo.tvdbEpisodeNumber.getValue());
    episodePostgres.episodeNumber.changeValue(episodeMongo.tvdbAbsoluteNumber.getValue());
    episodePostgres.airDate.changeValue(episodeMongo.tvdbFirstAired.getValue());
    episodePostgres.onTiVo.changeValue(episodeMongo.onTiVo.getValue());
    episodePostgres.watched.changeValue(episodeMongo.watched.getValue());
    episodePostgres.title.changeValue(episodeMongo.tivoEpisodeTitle.getValue());
    episodePostgres.seriesTitle.changeValue(episodeMongo.tivoSeriesTitle.getValue());
    episodePostgres.tivoProgramId.changeValue(episodeMongo.tivoProgramId.getValue());
  }

  private void copyAllTiVoEpisodeFields(EpisodeMongo episodeMongo, TiVoEpisodePostgres tiVoEpisodePostgres) {
    tiVoEpisodePostgres.suggestion.changeValue(episodeMongo.tivoSuggestion.getValue());
    tiVoEpisodePostgres.title.changeValue(episodeMongo.tivoEpisodeTitle.getValue());
    tiVoEpisodePostgres.showingStartTime.changeValue(episodeMongo.tivoShowingStartTime.getValue());
    tiVoEpisodePostgres.showingDuration.changeValue(episodeMongo.tivoShowingDuration.getValue());
    tiVoEpisodePostgres.deletedDate.changeValue(episodeMongo.tivoDeletedDate.getValue());
    tiVoEpisodePostgres.captureDate.changeValue(episodeMongo.tivoCaptureDate.getValue());
    tiVoEpisodePostgres.hd.changeValue(episodeMongo.tivoHD.getValue());
    tiVoEpisodePostgres.episodeNumber.changeValue(episodeMongo.tivoEpisodeNumber.getValue());
    tiVoEpisodePostgres.duration.changeValue(episodeMongo.tivoDuration.getValue());
    tiVoEpisodePostgres.channel.changeValue(episodeMongo.tivoChannel.getValue());
    tiVoEpisodePostgres.rating.changeValue(episodeMongo.tivoRating.getValue());
    tiVoEpisodePostgres.tivoSeriesId.changeValue(episodeMongo.tivoSeriesId.getValue());
    tiVoEpisodePostgres.programId.changeValue(episodeMongo.tivoProgramId.getValue());
    tiVoEpisodePostgres.description.changeValue(episodeMongo.tivoDescription.getValue());
    tiVoEpisodePostgres.station.changeValue(episodeMongo.tivoStation.getValue());
    tiVoEpisodePostgres.url.changeValue(episodeMongo.tivoUrl.getValue());
    tiVoEpisodePostgres.seriesTitle.changeValue(episodeMongo.tivoSeriesTitle.getValue());
  }

  private void copyAllTVDBEpisodeFields(EpisodeMongo episodeMongo, TVDBEpisodePostgres tvdbEpisodePostgres) {
    tvdbEpisodePostgres.seasonNumber.changeValue(episodeMongo.tvdbSeason.getValue());
    tvdbEpisodePostgres.seasonId.changeValue(episodeMongo.tvdbSeasonId.getValue());
    tvdbEpisodePostgres.tvdbId.changeValue(episodeMongo.tvdbEpisodeId.getValue());
    tvdbEpisodePostgres.episodeNumber.changeValue(episodeMongo.tvdbEpisodeNumber.getValue());
    tvdbEpisodePostgres.absoluteNumber.changeValue(episodeMongo.tvdbAbsoluteNumber.getValue());
    tvdbEpisodePostgres.ratingCount.changeValue(episodeMongo.tvdbRatingCount.getValue());
    tvdbEpisodePostgres.airsAfterSeason.changeValue(episodeMongo.tvdbAirsAfterSeason.getValue());
    tvdbEpisodePostgres.airsBeforeSeason.changeValue(episodeMongo.tvdbAirsBeforeSeason.getValue());
    tvdbEpisodePostgres.airsBeforeEpisode.changeValue(episodeMongo.tvdbAirsBeforeEpisode.getValue());
    tvdbEpisodePostgres.thumbHeight.changeValue(episodeMongo.tvdbThumbHeight.getValue());
    tvdbEpisodePostgres.thumbWidth.changeValue(episodeMongo.tvdbThumbWidth.getValue());
    tvdbEpisodePostgres.firstAired.changeValue(episodeMongo.tvdbFirstAired.getValue());
    tvdbEpisodePostgres.lastUpdated.changeValue(episodeMongo.tvdbLastUpdated.getValue());
    tvdbEpisodePostgres.rating.changeValue(episodeMongo.tvdbRating.getValue());
    tvdbEpisodePostgres.seriesName.changeValue(episodeMongo.tvdbSeriesName.getValue());
    tvdbEpisodePostgres.name.changeValue(episodeMongo.tvdbEpisodeName.getValue());
    tvdbEpisodePostgres.overview.changeValue(episodeMongo.tvdbOverview.getValue());
    tvdbEpisodePostgres.productionCode.changeValue(episodeMongo.tvdbProductionCode.getValue());
    tvdbEpisodePostgres.director.changeValue(episodeMongo.tvdbDirector.getValue());
    tvdbEpisodePostgres.writer.changeValue(episodeMongo.tvdbWriter.getValue());
    tvdbEpisodePostgres.filename.changeValue(episodeMongo.tvdbFilename.getValue());
  }

  private void copyAllTVDBSeriesFields(SeriesMongo seriesMongo, TVDBSeriesPostgres tvdbSeriesPostgres) {
    tvdbSeriesPostgres.firstAired.changeValue(seriesMongo.tvdbFirstAired.getValue());
    tvdbSeriesPostgres.tvdbId.changeValue(seriesMongo.tvdbId.getValue());
    tvdbSeriesPostgres.tvdbSeriesId.changeValue(seriesMongo.tvdbSeriesId.getValue());
    tvdbSeriesPostgres.ratingCount.changeValue(seriesMongo.tvdbRatingCount.getValue());
    tvdbSeriesPostgres.runtime.changeValue(seriesMongo.tvdbRuntime.getValue());
    tvdbSeriesPostgres.rating.changeValue(seriesMongo.tvdbRating.getValue());
    tvdbSeriesPostgres.name.changeValue(seriesMongo.tvdbName.getValue());
    tvdbSeriesPostgres.airsDayOfWeek.changeValue(seriesMongo.tvdbAirsDayOfWeek.getValue());
    tvdbSeriesPostgres.airsTime.changeValue(seriesMongo.tvdbAirsTime.getValue());
    tvdbSeriesPostgres.network.changeValue(seriesMongo.tvdbNetwork.getValue());
    tvdbSeriesPostgres.overview.changeValue(seriesMongo.tvdbOverview.getValue());
    tvdbSeriesPostgres.status.changeValue(seriesMongo.tvdbStatus.getValue());
    tvdbSeriesPostgres.poster.changeValue(seriesMongo.tvdbPoster.getValue());
    tvdbSeriesPostgres.banner.changeValue(seriesMongo.tvdbBanner.getValue());
    tvdbSeriesPostgres.lastUpdated.changeValue(seriesMongo.tvdbLastUpdated.getValue());
    tvdbSeriesPostgres.imdbId.changeValue(seriesMongo.imdbId.getValue());
    tvdbSeriesPostgres.zap2it_id.changeValue(seriesMongo.zap2it_id.getValue());
  }

  private void copyAllSeriesFields(SeriesMongo seriesMongo, SeriesPostgres seriesPostgres) {
    seriesPostgres.tier.changeValue(seriesMongo.tier.getValue());
    seriesPostgres.tivoSeriesId.changeValue(seriesMongo.tivoSeriesId.getValue());
    seriesPostgres.seriesTitle.changeValue(seriesMongo.seriesTitle.getValue());
    seriesPostgres.tvdbHint.changeValue(seriesMongo.tvdbHint.getValue());
    seriesPostgres.ignoreTVDB.changeValue(seriesMongo.ignoreTVDB.getValue());
    seriesPostgres.activeEpisodes.changeValue(seriesMongo.activeEpisodes.getValue());
    seriesPostgres.deletedEpisodes.changeValue(seriesMongo.deletedEpisodes.getValue());
    seriesPostgres.suggestionEpisodes.changeValue(seriesMongo.suggestionEpisodes.getValue());
    seriesPostgres.unmatchedEpisodes.changeValue(seriesMongo.unmatchedEpisodes.getValue());
    seriesPostgres.watchedEpisodes.changeValue(seriesMongo.watchedEpisodes.getValue());
    seriesPostgres.unwatchedEpisodes.changeValue(seriesMongo.unwatchedEpisodes.getValue());
    seriesPostgres.unwatchedUnrecorded.changeValue(seriesMongo.unwatchedUnrecorded.getValue());
    seriesPostgres.tvdbOnlyEpisodes.changeValue(seriesMongo.tvdbOnlyEpisodes.getValue());
    seriesPostgres.matchedEpisodes.changeValue(seriesMongo.matchedEpisodes.getValue());
    seriesPostgres.metacritic.changeValue(seriesMongo.metacritic.getValue());
    seriesPostgres.metacriticHint.changeValue(seriesMongo.metacriticHint.getValue());
    seriesPostgres.lastUnwatched.changeValue(seriesMongo.lastUnwatched.getValue());
    seriesPostgres.mostRecent.changeValue(seriesMongo.mostRecent.getValue());
    seriesPostgres.isSuggestion.changeValue(seriesMongo.isSuggestion.getValue());
    seriesPostgres.matchedWrong.changeValue(seriesMongo.matchedWrong.getValue());
    seriesPostgres.needsTVDBRedo.changeValue(seriesMongo.needsTVDBRedo.getValue());
  }

  private SeriesPostgres getOrCreateSeriesPostgres(String tivoSeriesId) throws SQLException {
    SeriesPostgres seriesPostgres = new SeriesPostgres();
    if (tivoSeriesId == null) {
      seriesPostgres.initializeForInsert();
      return seriesPostgres;
    }

    String sql = "SELECT * FROM series WHERE tivo_series_id = ?";
    ResultSet resultSet = postgresConnection.prepareAndExecuteStatementFetch(sql, tivoSeriesId);

    if (postgresConnection.hasMoreElements(resultSet)) {
      seriesPostgres.initializeFromDBObject(resultSet);
    } else {
      seriesPostgres.initializeForInsert();
    }
    return seriesPostgres;
  }

  private EpisodePostgres getOrCreateEpisodePostgres(String tivoProgramId) throws SQLException {
    EpisodePostgres episodePostgres = new EpisodePostgres();
    if (tivoProgramId == null) {
      episodePostgres.initializeForInsert();
      return episodePostgres;
    }

    String sql = "SELECT * FROM episode WHERE tivo_program_id = ?";
    ResultSet resultSet = postgresConnection.prepareAndExecuteStatementFetch(sql, tivoProgramId);

    if (postgresConnection.hasMoreElements(resultSet)) {
      episodePostgres.initializeFromDBObject(resultSet);
    } else {
      episodePostgres.initializeForInsert();
    }
    return episodePostgres;
  }

  private TiVoEpisodePostgres getOrCreateTiVoEpisodePostgres(String tivoProgramId) throws SQLException {
    TiVoEpisodePostgres tiVoEpisodePostgres = new TiVoEpisodePostgres();
    if (tivoProgramId == null) {
      tiVoEpisodePostgres.initializeForInsert();
      return tiVoEpisodePostgres;
    }

    String sql = "SELECT * FROM tivo_episode WHERE program_id = ?";
    ResultSet resultSet = postgresConnection.prepareAndExecuteStatementFetch(sql, tivoProgramId);

    if (postgresConnection.hasMoreElements(resultSet)) {
      tiVoEpisodePostgres.initializeFromDBObject(resultSet);
    } else {
      tiVoEpisodePostgres.initializeForInsert();
    }
    return tiVoEpisodePostgres;
  }

  private TVDBEpisodePostgres getOrCreateTVDBEpisodePostgres(Integer tvdbEpisodeId) throws SQLException {
    TVDBEpisodePostgres tvdbEpisodePostgres = new TVDBEpisodePostgres();
    if (tvdbEpisodeId == null) {
      tvdbEpisodePostgres.initializeForInsert();
      return tvdbEpisodePostgres;
    }

    String sql = "SELECT * FROM tvdb_episode WHERE tvdb_id = ?";
    ResultSet resultSet = postgresConnection.prepareAndExecuteStatementFetch(sql, tvdbEpisodeId);

    if (postgresConnection.hasMoreElements(resultSet)) {
      tvdbEpisodePostgres.initializeFromDBObject(resultSet);
    } else {
      tvdbEpisodePostgres.initializeForInsert();
    }
    return tvdbEpisodePostgres;
  }

  private TVDBSeriesPostgres getOrCreateTVDBSeriesPostgres(Integer tvdbId) throws SQLException {
    TVDBSeriesPostgres tvdbSeriesPostgres = new TVDBSeriesPostgres();
    if (tvdbId == null) {
      tvdbSeriesPostgres.initializeForInsert();
      return tvdbSeriesPostgres;
    }

    String sql = "SELECT * FROM tvdb_series WHERE tvdb_id = ?";
    ResultSet resultSet = postgresConnection.prepareAndExecuteStatementFetch(sql, tvdbId);

    if (postgresConnection.hasMoreElements(resultSet)) {
      tvdbSeriesPostgres.initializeFromDBObject(resultSet);
    } else {
      tvdbSeriesPostgres.initializeForInsert();
    }
    return tvdbSeriesPostgres;
  }

  protected void debug(Object object) {
    System.out.println(object);
  }

}
