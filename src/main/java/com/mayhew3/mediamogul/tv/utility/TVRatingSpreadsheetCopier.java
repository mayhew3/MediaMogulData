package com.mayhew3.mediamogul.tv.utility;

import com.mayhew3.mediamogul.model.tv.Episode;
import com.mayhew3.mediamogul.model.tv.EpisodeRating;
import com.mayhew3.mediamogul.model.tv.Series;
import com.mayhew3.mediamogul.model.tv.TmpRating;
import com.mayhew3.mediamogul.tv.SeriesDenormUpdater;
import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;

public class TVRatingSpreadsheetCopier {
  private SQLConnection connection;

  private static Logger logger = LogManager.getLogger(TVRatingSpreadsheetCopier.class);

  private TVRatingSpreadsheetCopier(SQLConnection connection) {
    this.connection = connection;
  }

  public static void main(String... args) throws URISyntaxException, SQLException {
    ArgumentChecker argumentChecker = new ArgumentChecker(args);

    SQLConnection connection = PostgresConnectionFactory.createConnection(argumentChecker);
    TVRatingSpreadsheetCopier spreadsheetCopier = new TVRatingSpreadsheetCopier(connection);
    spreadsheetCopier.runUpdate();

    new SeriesDenormUpdater(connection).runUpdate();
}

  private void runUpdate() throws SQLException {
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(
        "SELECT * " +
            "FROM tmp_rating " +
            "WHERE episode_id IS NULL"
    );
    while (resultSet.next()) {
      TmpRating tmpRating = new TmpRating();
      tmpRating.initializeFromDBObject(resultSet);

      try {
        Series series = findSeriesMatch(tmpRating);
        if (series != null) {
          tmpRating.seriesId.changeValue(series.id.getValue());
          tmpRating.commit(connection);

          Episode episode = findEpisodeMatch(tmpRating, series);
          if (episode != null) {
            copyFieldsToEpisode(episode, tmpRating);
            tmpRating.episodeId.changeValue(episode.id.getValue());
            tmpRating.commit(connection);

//            debug("Episode matched and rating updated. " + tmpRating);
          } else {
            logger.warn("Series found, but episode failed to match. " + tmpRating);
          }
        } else {
          logger.warn("No series match found. " + tmpRating);
        }
      } catch (Exception e) {
        logger.warn("Row in spreadsheet failed. " + tmpRating);
        e.printStackTrace();
      }
    }
  }

  private void copyFieldsToEpisode(@NotNull Episode episode, @NotNull TmpRating tmpRating) throws SQLException {
    Timestamp watchedDate = tmpRating.watchedDate.getValue();
    if (watchedDate != null) {
      watchedDate = new Timestamp(DateUtils.addHours(watchedDate, 21).getTime());

      episode.watched.changeValue(true);

      Timestamp existingDate = episode.watchedDate.getValue();

      if (existingDate != null) {
        Date truncatedExisting = DateUtils.truncate(existingDate, Calendar.DAY_OF_MONTH);
        Date truncatedSpreadsheet = DateUtils.truncate(watchedDate, Calendar.DAY_OF_MONTH);

        if (truncatedSpreadsheet.before(truncatedExisting)) {
          episode.watchedDate.changeValue(watchedDate);
        }
      } else {
        episode.watchedDate.changeValue(watchedDate);
      }

      episode.commit(connection);
    }

    BigDecimal ratingValue = tmpRating.rating.getValue();

    if (ratingValue != null) {

      @Nullable EpisodeRating existingRating = findExistingRating(episode);

      if (existingRating == null) {
        existingRating = new EpisodeRating();
        existingRating.initializeForInsert();

        existingRating.episodeId.changeValue(episode.id.getValue());
        existingRating.dateAdded.changeValue(new Date());
      }

      existingRating.ratingFunny.changeValue(multiplyAndRound(tmpRating.funny.getValue()));
      existingRating.ratingCharacter.changeValue(multiplyAndRound(tmpRating.character.getValue()));
      existingRating.ratingStory.changeValue(multiplyAndRound(tmpRating.story.getValue()));
      existingRating.ratingValue.changeValue(multiplyAndRound(ratingValue));

      existingRating.ratingDate.changeValue(watchedDate);

      String review = tmpRating.note.getValue();
      if ("".equals(review)) {
        review = null;
      }
      existingRating.review.changeValue(review);

      existingRating.commit(connection);
    }
  }

  @Nullable
  private BigDecimal multiplyAndRound(@Nullable BigDecimal original) {
    if (original == null) {
      return null;
    }
    BigDecimal timesTen = original.multiply(BigDecimal.valueOf(10));
    return timesTen.setScale(0, RoundingMode.HALF_EVEN);
  }


  @Nullable
  private EpisodeRating findExistingRating(Episode episode) throws SQLException {
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(
        "SELECT * " +
            "FROM episode_rating " +
            "WHERE episode_id = ?",
        episode.id.getValue()
    );
    if (resultSet.next()) {
      EpisodeRating episodeRating = new EpisodeRating();
      episodeRating.initializeFromDBObject(resultSet);

      return episodeRating;
    }
    return null;
  }

  @Nullable
  private Episode findEpisodeMatch(TmpRating tmpRating, Series series) throws SQLException {
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(
        "SELECT * " +
            "FROM episode " +
            "WHERE series_id = ? " +
            "AND season = ? " +
            "AND episode_number = ? " +
            "AND retired = ? ",
        series.id.getValue(), tmpRating.seasonNumber.getValue(), tmpRating.episodeNumber.getValue(), 0
    );
    if (resultSet.next()) {
      Episode episode = new Episode();
      episode.initializeFromDBObject(resultSet);

      return episode;
    }
    return null;
  }

  @Nullable
  private Series findSeriesMatch(TmpRating tmpRating) throws SQLException {
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(
        "SELECT * " +
            "FROM series " +
            "WHERE title = ? " +
            "and retired = ? ",
        tmpRating.seriesTitle.getValue(), 0
    );
    if (resultSet.next()) {
      Series series = new Series();
      series.initializeFromDBObject(resultSet);
      return series;
    }
    return null;
  }

  public void debug(Object message) {
    logger.debug(message);
  }
}
