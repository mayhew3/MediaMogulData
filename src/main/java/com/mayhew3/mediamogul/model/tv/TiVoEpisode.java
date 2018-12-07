package com.mayhew3.mediamogul.model.tv;

import com.mayhew3.postgresobject.dataobject.*;
import com.mayhew3.postgresobject.db.SQLConnection;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class TiVoEpisode extends RetireableDataObject {

  public FieldValueBoolean suggestion = registerBooleanField("suggestion", Nullability.NULLABLE);

  public FieldValueString title = registerStringField("title", Nullability.NULLABLE);

  public FieldValueTimestamp showingStartTime = registerTimestampField("showing_start_time", Nullability.NULLABLE);
  public FieldValueTimestamp deletedDate = registerTimestampField("deleted_date", Nullability.NULLABLE);
  public FieldValueTimestamp captureDate = registerTimestampField("capture_date", Nullability.NULLABLE);

  public FieldValueInteger tvdbMatchId = registerIntegerField("tvdb_match_id", Nullability.NULLABLE);
  public FieldValueString tvdbMatchStatus = registerStringField("tvdb_match_status", Nullability.NULLABLE);

  public FieldValue<Boolean> hd = registerBooleanField("hd", Nullability.NULLABLE);

  public FieldValue<Integer> episodeNumber = registerIntegerField("episode_number", Nullability.NULLABLE);
  public FieldValue<Integer> duration = registerIntegerField("duration", Nullability.NULLABLE);
  public FieldValue<Integer> showingDuration = registerIntegerField("showing_duration", Nullability.NULLABLE);
  public FieldValue<Integer> channel = registerIntegerField("channel", Nullability.NULLABLE);
  public FieldValue<Integer> rating = registerIntegerField("rating", Nullability.NULLABLE);

  public FieldValue<String> tivoSeriesExtId = registerStringField("tivo_series_ext_id", Nullability.NULLABLE);
  public FieldValue<String> tivoSeriesV2ExtId = registerStringField("tivo_series_v2_ext_id", Nullability.NULLABLE);
  public FieldValue<String> programId = registerStringField("program_id", Nullability.NULLABLE);
  public FieldValue<String> programV2Id = registerStringField("program_v2_id", Nullability.NULLABLE);
  public FieldValue<String> seriesTitle = registerStringField("series_title", Nullability.NULLABLE);
  public FieldValue<String> description = registerStringField("description", Nullability.NULLABLE);
  public FieldValue<String> station = registerStringField("station", Nullability.NULLABLE);
  public FieldValue<String> url = registerStringField("url", Nullability.NULLABLE);

  public FieldValueBoolean recordingNow = registerBooleanField("recording_now", Nullability.NOT_NULL);
  public FieldValueBoolean ignoreMatching = registerBooleanField("ignore_matching", Nullability.NOT_NULL).defaultValue(false);

  public FieldValueInteger tivoVersion = registerIntegerField("tivo_version", Nullability.NOT_NULL).defaultValue(1);


  public TiVoEpisode() {
    addUniqueConstraint(programId, programV2Id, captureDate, retired);
    addUniqueConstraint(captureDate, seriesTitle, retired);
  }

  @Override
  public String getTableName() {
    return "tivo_episode";
  }

  @Override
  public String toString() {
    return seriesTitle.getValue() + " " + episodeNumber.getValue() + ": " + title.getValue();
  }

  public List<Episode> getEpisodes(SQLConnection connection) throws SQLException {
    List<Episode> episodes = new ArrayList<>();
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(
        "SELECT e.* " +
            "FROM episode e " +
            "INNER JOIN edge_tivo_episode ete " +
            " ON ete.episode_id = e.id " +
            "WHERE ete.tivo_episode_id = ? " +
            "AND e.retired = ? ", id.getValue(), 0);

    while (resultSet.next()) {
      Episode episode = new Episode();
      episode.initializeFromDBObject(resultSet);
      episodes.add(episode);
    }
    return episodes;
  }
}
