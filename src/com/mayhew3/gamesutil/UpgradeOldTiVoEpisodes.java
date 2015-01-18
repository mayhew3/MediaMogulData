package com.mayhew3.gamesutil;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

public class UpgradeOldTiVoEpisodes extends TVDatabaseUtility {

  private Map<String, String> newColumns;

  public UpgradeOldTiVoEpisodes() throws UnknownHostException {
    super("tv");

    newColumns = new HashMap<>();
    newColumns.put("AddedDate", "DateAdded");
    newColumns.put("CaptureDate", "TiVoCaptureDate");
    newColumns.put("DeletedDate", "TiVoDeletedDate");
    newColumns.put("Description", "TiVoDescription");
    newColumns.put("EpisodeTitle", "TiVoEpisodeTitle");
    newColumns.put("EpisodeNumber", "TiVoEpisodeNumber");
    newColumns.put("HighDefinition", "TiVoHD");
    newColumns.put("ProgramId", "TiVoProgramId");
    newColumns.put("ShowingDuration", "TiVoShowingDuration");
    newColumns.put("ShowingStartTime", "TiVoShowingStartTime");
    newColumns.put("SourceChannel", "TiVoChannel");
    newColumns.put("SourceStation", "TiVoStation");
    newColumns.put("Suggestion", "TiVoSuggestion");
    newColumns.put("Title", "TiVoSeriesTitle");
    newColumns.put("TvRating", "TiVoRating");
    newColumns.put("Url", "TiVoUrl");

  }

  // declare mappings from old -> new column names
  // utility should copy the values over, and delete the old field.
  public static void main(String[] args) {
    try {
      UpgradeOldTiVoEpisodes tiVoEpisodes = new UpgradeOldTiVoEpisodes();
      tiVoEpisodes.upgradeRows();
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }

  }

  public void upgradeRows() {
    DBCursor episodes = _db.getCollection("episodes")
        .find(new BasicDBObject()
                .append("TiVoSeriesTitle", new BasicDBObject("$exists", false))
//            .append("Suggestion", new BasicDBObject("$ne", true))
//            .append("Suggestion", false)
//            .append("DeletedDate", new BasicDBObject("$exists", false))
        );

    while(episodes.hasNext()) {

      DBObject episode = episodes.next();

      Object programId = getProgramId(episode);
      debug("Updating fields for Program ID '" + programId + "'");

      Object seriesTitle = getSeriesTitle(episode);

      if (seriesTitle == null) {
        updateSeriesTitle(episode);
      }

    }

  }

  private Boolean isEmpty(Object value) {
    return value == null || "".equals(value);
  }

  private Object getEpisodeTitle(DBObject episode) {
    Object episodeTitle = episode.get("TiVoEpisodeTitle");
    if (episodeTitle == null) {
      episodeTitle = episode.get("EpisodeTitle");
      if (episodeTitle == null) {
        episodeTitle = episode.get("tvdbEpisodeName");
        if (episodeTitle == null) {
          debug("No episode title found for Episode with ID " + episode.get("_id"));
        }
      }
    }
    return episodeTitle;
  }

  private Object getProgramId(DBObject episode) {
    Object episodeTitle = episode.get("TiVoProgramId");
    if (episodeTitle == null) {
      episodeTitle = episode.get("ProgramId");
      if (episodeTitle == null) {
        throw new RuntimeException("No program id found for Episode with ID " + episode.get("_id"));
      }
    }
    return episodeTitle;
  }

  private Object getSeriesTitle(DBObject episode) {
    return episode.get("TiVoSeriesTitle");
  }

  private DBObject updateSeriesId(DBObject episode) {
    Object tivoSeriesId = episode.get("SeriesId");
    DBObject seriesObject = findSingleMatch(_db.getCollection("series"), "SeriesId", tivoSeriesId);

    if (seriesObject == null) {
      seriesObject = insertNewSeries(episode, tivoSeriesId);
    }
    Object seriesId = seriesObject.get("_id");
    Object episodeId = episode.get("_id");
    singleFieldUpdateWithId("episodes", episodeId, "TiVoSeriesId", tivoSeriesId);
    singleFieldUpdateWithId("episodes", episodeId, "SeriesId", seriesId);

    return seriesObject;
  }

  private DBObject insertNewSeries(DBObject episode, Object tivoSeriesId) {
    Object seriesTitle = episode.get("Title");
    if (seriesTitle == null) {
      throw new RuntimeException("Nowhere to get series title from!");
    }

    debug("Adding series '" + seriesTitle + "'  with TiVoID '" + tivoSeriesId + "'");

    BasicDBObject seriesInsert = new BasicDBObject();

    Object suggestion = episode.get("Suggestion");
    Boolean isSuggestion = suggestion != null && Boolean.TRUE.equals(suggestion);
    Boolean isEpisodic = true;

    Integer tier = isSuggestion ? 5 : 4;

    seriesInsert.append("SeriesId", tivoSeriesId);
    seriesInsert.append("SeriesTitle", seriesTitle);
    seriesInsert.append("TiVoName", seriesTitle);
    seriesInsert.append("IsEpisodic", isEpisodic);
    seriesInsert.append("IsSuggestion", isSuggestion);
    seriesInsert.append("Tier", tier);
    seriesInsert.append("DateAdded", episode.get("AddedDate"));

    DBCollection collection = _db.getCollection("series");
    collection.insert(seriesInsert);

    Object id = seriesInsert.get("_id");

    return findSingleMatch(collection, "_id", id);
  }

  private Object updateSeriesTitle(DBObject episode) {
    Object seriesId = episode.get("SeriesId");
    DBObject seriesObject = findSingleMatch(_db.getCollection("series"), "_id", seriesId);
    if (seriesObject == null) {
      seriesObject = updateSeriesId(episode);
    }
    Object tiVoName = seriesObject.get("TiVoName");
    if (tiVoName == null) {
      tiVoName = seriesObject.get("SeriesTitle");
      if (tiVoName == null) {
        throw new RuntimeException("No name found for Series with _id " + seriesId);
      }
    }

    debug("Found series name '" + tiVoName + "', updating now!");
    singleFieldUpdateWithId("episodes", episode.get("_id"), "TiVoSeriesTitle", tiVoName);

    return tiVoName;
  }


}
