package com.mayhew3.mediamogul.tv;

import com.cloudinary.Cloudinary;
import com.cloudinary.utils.ObjectUtils;
import com.google.common.collect.Lists;
import com.mayhew3.mediamogul.model.tv.Series;
import com.mayhew3.mediamogul.model.tv.TVDBPoster;
import com.mayhew3.mediamogul.model.tv.TVDBSeries;
import com.mayhew3.postgresobject.db.SQLConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class CloudinaryUpdater {

  private Cloudinary cloudinary;
  private Series series;
  private SQLConnection connection;

  private static Logger logger = LogManager.getLogger(CloudinaryUploadRunner.class);

  public CloudinaryUpdater(Cloudinary cloudinary, Series series, SQLConnection connection) {
    this.cloudinary = cloudinary;
    this.series = series;
    this.connection = connection;
  }


  public void updateSeries(Boolean otherPosters) throws SQLException {
    debug("Updating posters for series '" + series.seriesTitle.getValue() + "'...");
    String poster = series.poster.getValue();
    String cloud_poster = series.cloud_poster.getValue();
    if (poster != null && cloud_poster == null) {
      String url = "https://thetvdb.com/banners/" + poster;
      Optional<String> maybeCloudID = uploadToCloudinaryAndReturnPublicID(url);
      if (maybeCloudID.isPresent()) {
        debug("Successfully uploaded poster for series '" + series.seriesTitle.getValue() + "'");
        String cloudID = maybeCloudID.get();

        series.cloud_poster.changeValue(cloudID);
        series.commit(connection);


      }
    }
    if (otherPosters) {
      updateAllTVDBPosters();
    }
  }

  private void updateAllTVDBPosters() throws SQLException {
    Optional<TVDBSeries> maybeTVDBSeries = series.getTVDBSeries(connection);
    if (maybeTVDBSeries.isPresent()) {
      TVDBSeries tvdbSeries = maybeTVDBSeries.get();
      String sql = "SELECT * " +
          "FROM tvdb_poster " +
          "WHERE tvdb_series_id = ? " +
          "AND retired = ? ";
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, tvdbSeries.id.getValue(), 0);
      while (resultSet.next()) {
        TVDBPoster tvdbPoster = new TVDBPoster();
        tvdbPoster.initializeFromDBObject(resultSet);
        updateTVDBPoster(tvdbPoster);
      }
    }
  }

  private void updateTVDBPoster(TVDBPoster tvdbPoster) throws SQLException {
    String poster = tvdbPoster.posterPath.getValue();
    String cloud_poster = tvdbPoster.cloud_poster.getValue();
    if (poster != null && cloud_poster == null) {
      debug("Uploading poster '" + poster + "'...");
      String url = "https://thetvdb.com/banners/" + poster;
      Optional<String> maybeCloudID = uploadToCloudinaryAndReturnPublicID(url);
      if (maybeCloudID.isPresent()) {
        tvdbPoster.cloud_poster.changeValue(maybeCloudID.get());
        tvdbPoster.commit(connection);
      }
    }
  }

  private Optional<String> uploadToCloudinaryAndReturnPublicID(String url) {
    if (exists(url)) {
      try {
        Map uploadResult = cloudinary.uploader().upload(url, ObjectUtils.emptyMap());
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
