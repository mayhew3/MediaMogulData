package com.mayhew3.mediamogul.tv;

import com.cloudinary.Cloudinary;
import com.cloudinary.utils.ObjectUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.mayhew3.mediamogul.model.tv.Series;
import com.mayhew3.mediamogul.model.tv.TVDBPoster;
import com.mayhew3.mediamogul.model.tv.TVDBSeries;
import com.mayhew3.mediamogul.tv.exception.ShowFailedException;
import com.mayhew3.postgresobject.db.SQLConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class CloudinaryUpdater {

  private Cloudinary cloudinary;
  private Series series;
  private TVDBPoster poster;
  private SQLConnection connection;

  private static Logger logger = LogManager.getLogger(CloudinaryUploadRunner.class);

  public CloudinaryUpdater(Cloudinary cloudinary, Series series, SQLConnection connection) {
    this.cloudinary = cloudinary;
    this.series = series;
    this.connection = connection;
  }

  public CloudinaryUpdater(Cloudinary cloudinary, TVDBPoster poster, SQLConnection connection) {
    this.cloudinary = cloudinary;
    this.poster = poster;
    this.connection = connection;
  }

  public void updateSeries() throws SQLException, ShowFailedException {
    Preconditions.checkState(series != null, "Cannot call updateSeries without initializing series object.");
    debug("Updating posters for series '" + series.seriesTitle.getValue() + "'...");

    List<TVDBPoster> tvdbPosters = updateAllTVDBPosters();
    Optional<TVDBSeries> maybeTVDBSeries = series.getTVDBSeries(connection);
    Optional<TVDBPoster> maybeFirstWorkingPoster = tvdbPosters.stream()
        .filter(tvdbPoster -> tvdbPoster.cloud_poster.getValue() != null)
        .findFirst();
    if (maybeFirstWorkingPoster.isPresent()) {

      TVDBPoster firstWorkingPoster = maybeFirstWorkingPoster.get();
      if (maybeTVDBSeries.isPresent()) {
        TVDBSeries tvdbSeries = maybeTVDBSeries.get();

        boolean isOverridden = isOverridden(tvdbSeries);

        tvdbSeries.firstPoster.changeValue(firstWorkingPoster.posterPath.getValue());
        tvdbSeries.commit(connection);

        if (!isOverridden) {
          series.tvdbPosterId.changeValue(firstWorkingPoster.id.getValue());
          series.commit(connection);
        } else {
          Optional<TVDBPoster> linkedPoster = series.getLinkedPoster(connection);
          if (linkedPoster.isPresent() && series.tvdbPosterId.getValue() == null) {
            series.tvdbPosterId.changeValue(linkedPoster.get().id.getValue());
            series.commit(connection);
          }
        }
      }

    } else {
      throw new ShowFailedException("No working poster found for series: " + series.seriesTitle.getValue());
    }
  }

  private boolean isOverridden(TVDBSeries tvdbSeries) throws SQLException {
    Optional<TVDBPoster> optionalSeriesPoster = series.getTVDBPoster(connection);
    String tvdbPoster;
    String seriesPosterPath;
    if (optionalSeriesPoster.isPresent()) {
      tvdbPoster = tvdbSeries.firstPoster.getValue();
      seriesPosterPath = optionalSeriesPoster.get().posterPath.getValue();
    } else {
      tvdbPoster = tvdbSeries.lastPoster.getValue();
      seriesPosterPath = series.poster.getValue();
    }
    return tvdbPoster != null &&
        !tvdbPoster.equals(seriesPosterPath);
  }

  private List<TVDBPoster> updateAllTVDBPosters() throws SQLException {
    Optional<TVDBSeries> maybeTVDBSeries = series.getTVDBSeries(connection);
    List<TVDBPoster> posters = new ArrayList<>();
    if (maybeTVDBSeries.isPresent()) {
      TVDBSeries tvdbSeries = maybeTVDBSeries.get();
      String sql = "SELECT * " +
          "FROM tvdb_poster " +
          "WHERE tvdb_series_id = ? " +
          "AND retired = ? " +
          "AND hidden IS NULL " +
          "ORDER BY id ";
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, tvdbSeries.id.getValue(), 0);
      while (resultSet.next()) {
        TVDBPoster tvdbPoster = new TVDBPoster();
        tvdbPoster.initializeFromDBObject(resultSet);
        posters.add(tvdbPoster);
        try {
          updateTVDBPoster(tvdbPoster);
        } catch (ShowFailedException e) {
          e.printStackTrace();
          logger.warn("Poster failed while updating series. Continuing on other posters.");
        }
      }

      Optional<TVDBPoster> linkedPoster = series.getLinkedPoster(connection);
      if (linkedPoster.isEmpty() && series.cloud_poster.getValue() != null) {
        TVDBPoster tvdbPoster = new TVDBPoster();
        tvdbPoster.initializeForInsert();
        tvdbPoster.tvdb_series_id.changeValue(series.tvdbSeriesId.getValue());
        tvdbPoster.posterPath.changeValue(series.poster.getValue());
        tvdbPoster.cloud_poster.changeValue(series.cloud_poster.getValue());
        tvdbPoster.commit(connection);

        posters.add(tvdbPoster);
      }
    }
    return posters;
  }

  public void updateTVDBPoster() throws SQLException, ShowFailedException {
    Preconditions.checkState(poster != null, "Cannot call method without initializing Poster first.");
    updateTVDBPoster(poster);
  }

  private void updateTVDBPoster(TVDBPoster tvdbPoster) throws SQLException, ShowFailedException {
    String poster = tvdbPoster.posterPath.getValue();
    String cloud_poster = tvdbPoster.cloud_poster.getValue();
    //noinspection ConstantConditions
    if (poster != null && cloud_poster == null &&
        !"".equals(poster) && !"".equals(cloud_poster)) {
      debug("Uploading poster '" + poster + "'...");
      String url = "https://thetvdb.com/banners/" + poster;
      Optional<String> maybeCloudID = uploadToCloudinaryAndReturnPublicID(url);
      if (maybeCloudID.isPresent()) {
        tvdbPoster.cloud_poster.changeValue(maybeCloudID.get());
        tvdbPoster.commit(connection);
      } else {
        throw new ShowFailedException("TVDB Poster not found.");
      }
    }
  }

  private Optional<String> uploadToCloudinaryAndReturnPublicID(String url) {
    if (exists(url)) {
      try {
        @SuppressWarnings("rawtypes") Map uploadResult = cloudinary.uploader().upload(url, ObjectUtils.emptyMap());
        return Optional.of(uploadResult.get("public_id").toString());
      } catch (Exception e) {
        e.printStackTrace();
        logger.warn("Failed to resolve url even though it passed exists check: '" + url + "'");
        return Optional.empty();
      }
    } else {
      logger.warn("TVDB image not available at the moment: " + url);
      return Optional.empty();
    }
  }

  private void debug(Object message) {
    logger.debug(message);
  }

  private static boolean exists(String URLName){
    List<Integer> acceptableCodes = Lists.newArrayList(HttpURLConnection.HTTP_OK, HttpURLConnection.HTTP_MOVED_PERM);
    try {
      HttpURLConnection.setFollowRedirects(false);
      // note : you may also need
      //        HttpURLConnection.setInstanceFollowRedirects(false)
      HttpURLConnection con =
          (HttpURLConnection) new URL(URLName).openConnection();
      con.setRequestMethod("HEAD");
      return (acceptableCodes.contains(con.getResponseCode()));
    }
    catch (Exception e) {
      e.printStackTrace();
      return false;
    }
  }

}
