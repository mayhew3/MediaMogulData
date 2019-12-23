package com.mayhew3.mediamogul.model.tv;

import com.mayhew3.mediamogul.tv.exception.ShowFailedException;
import com.mayhew3.postgresobject.dataobject.*;
import com.mayhew3.mediamogul.model.Person;
import com.mayhew3.postgresobject.db.SQLConnection;

import java.sql.ResultSet;
import java.sql.SQLException;

public class EpisodeRating extends RetireableDataObject {

  public FieldValueForeignKey episodeId = registerForeignKey(new Episode(), Nullability.NOT_NULL);

  public FieldValueBoolean watched = registerBooleanField("watched", Nullability.NOT_NULL).defaultValue(true);
  public FieldValueTimestamp watchedDate = registerTimestampField("watched_date", Nullability.NULLABLE);
  public FieldValueTimestamp ratingDate = registerTimestampField("rating_date", Nullability.NULLABLE);

  public FieldValueBigDecimal ratingFunny = registerBigDecimalField("rating_funny", Nullability.NULLABLE);
  public FieldValueBigDecimal ratingCharacter = registerBigDecimalField("rating_character", Nullability.NULLABLE);
  public FieldValueBigDecimal ratingStory = registerBigDecimalField("rating_story", Nullability.NULLABLE);

  public FieldValueBigDecimal ratingValue = registerBigDecimalField("rating_value", Nullability.NULLABLE);

  public FieldValueString review = registerStringField("review", Nullability.NULLABLE);
  public FieldValueForeignKey personId = registerForeignKey(new Person(), Nullability.NOT_NULL);

  public EpisodeRating() {
    registerBooleanField("rating_pending", Nullability.NOT_NULL).defaultValue(false);
    addColumnsIndex(episodeId, personId, retired);
    addColumnsIndex(watchedDate);
  }

  @Override
  public String getTableName() {
    return "episode_rating";
  }

  public Episode getEpisode(SQLConnection connection) throws SQLException, ShowFailedException {
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch("SELECT * FROM episode WHERE id = ?", episodeId.getValue());

    if (resultSet.next()) {
      Episode episode = new Episode();
      episode.initializeFromDBObject(resultSet);
      return episode;
    } else {
      throw new ShowFailedException("EpisodeRating " + id.getValue() + " has episode_id " + episodeId.getValue() + " that wasn't found.");
    }
  }

}
