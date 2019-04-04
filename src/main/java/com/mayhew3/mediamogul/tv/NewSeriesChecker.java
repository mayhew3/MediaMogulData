package com.mayhew3.mediamogul.tv;

import com.mashape.unirest.http.exceptions.UnirestException;
import com.mayhew3.mediamogul.model.tv.Series;
import com.mayhew3.mediamogul.scheduler.UpdateRunner;
import com.mayhew3.mediamogul.tv.exception.ShowFailedException;
import com.mayhew3.mediamogul.tv.helper.UpdateMode;
import com.mayhew3.mediamogul.tv.provider.TVDBJWTProvider;
import com.mayhew3.mediamogul.xml.JSONReader;
import com.mayhew3.postgresobject.db.SQLConnection;
import org.apache.http.auth.AuthenticationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import java.sql.ResultSet;
import java.sql.SQLException;

public class NewSeriesChecker implements UpdateRunner {

  private SQLConnection connection;
  private TVDBJWTProvider tvdbjwtProvider;
  private JSONReader jsonReader;

  private static Logger logger = LogManager.getLogger(NewSeriesChecker.class);

  public NewSeriesChecker(SQLConnection connection, TVDBJWTProvider tvdbjwtProvider, JSONReader jsonReader) {
    this.connection = connection;
    this.tvdbjwtProvider = tvdbjwtProvider;
    this.jsonReader = jsonReader;
  }

  @Override
  public String getRunnerName() {
    return "New Series Checker";
  }

  @Override
  public @Nullable UpdateMode getUpdateMode() {
    return null;
  }

  @Override
  public void runUpdate() throws SQLException, UnirestException, AuthenticationException {
    String sql = "SELECT * " +
        "FROM series " +
        "WHERE first_processed = ? ";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, false);

    while (resultSet.next()) {
      Series series = new Series();
      series.initializeFromDBObject(resultSet);

      // DO STUFF
      logger.info("New Series found: '" + series.seriesTitle.getValue() + "'");

      updateTVDB(series);

      series.firstProcessed.changeValue(true);
      series.commit(connection);
    }
  }

  private void updateTVDB(Series series) throws SQLException, UnirestException, AuthenticationException {
    TVDBSeriesUpdater updater = new TVDBSeriesUpdater(connection, series, tvdbjwtProvider, jsonReader);
    try {
      updater.updateSeries();
    } catch (ShowFailedException e) {
      e.printStackTrace();
    }
  }
}
