package com.mayhew3.mediamogul.tv;

import com.cloudinary.Cloudinary;
import com.cloudinary.Singleton;
import com.cloudinary.utils.ObjectUtils;
import com.google.common.collect.Lists;
import com.mayhew3.mediamogul.model.tv.Series;
import com.mayhew3.mediamogul.model.tv.TVDBPoster;
import com.mayhew3.mediamogul.model.tv.TVDBSeries;
import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class CloudinaryUploader {

  private Cloudinary cloudinary;
  private SQLConnection connection;

  public CloudinaryUploader(SQLConnection connection) {
    this.connection = connection;
    this.cloudinary = Singleton.getCloudinary();
  }

  public static void main(String... args) throws URISyntaxException, SQLException {
    ArgumentChecker argumentChecker = new ArgumentChecker(args);
    SQLConnection connection = PostgresConnectionFactory.createConnection(argumentChecker);
    CloudinaryUploader cloudinaryUploader = new CloudinaryUploader(connection);
    cloudinaryUploader.runUpdateOnTierOne();
  }

  public void runUpdateSingle() throws SQLException {
    String sql = "SELECT * " +
            "FROM series " +
            "WHERE title = ? ";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, "You're the Worst");
    runUpdateOnResultSet(resultSet, true);
  }

  public void runUpdateOnTierOne() throws SQLException {
    String sql = "SELECT s.* " +
            "FROM series s " +
            "INNER JOIN person_series ps " +
            "  ON ps.series_id = s.id " +
            "WHERE ps.tier = ? " +
            "AND ps.person_id = ? ";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, 1, 1);
    runUpdateOnResultSet(resultSet, true);
  }

  public void runUpdateOnAllNonSuggestionSeries() throws SQLException {
    String sql = "SELECT s.* " +
            "FROM series s " +
            "WHERE s.suggestion = ? " +
            "AND s.retired = ? ";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, false, 0);
    runUpdateOnResultSet(resultSet, false);
  }

  public void runUpdateOnResultSet(ResultSet resultSet, Boolean otherPosters) throws SQLException {
    while (resultSet.next()) {
      Series series = new Series();
      series.initializeFromDBObject(resultSet);

      updateSeries(series, otherPosters);
    }
  }

  public void updateSeries(Series series, Boolean otherPosters) throws SQLException {
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
      updateAllTVDBPosters(series);
    }
  }

  private void updateAllTVDBPosters(Series series) throws SQLException {
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

  public void updateTVDBPoster(TVDBPoster tvdbPoster) throws SQLException {
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
        debug("Failed to resolve url even though it passed exists check: '" + url + "'");
        return Optional.empty();
      }
    } else {
      debug("TVDB image not available at the moment: " + url);
      return Optional.empty();
    }
  }

  protected void debug(Object object) {
    System.out.println(object);
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
