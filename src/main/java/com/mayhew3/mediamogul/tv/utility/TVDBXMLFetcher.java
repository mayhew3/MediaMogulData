package com.mayhew3.mediamogul.tv.utility;

import com.mayhew3.mediamogul.EnvironmentChecker;
import com.mayhew3.mediamogul.exception.MissingEnvException;
import com.mayhew3.mediamogul.model.tv.Series;
import com.mayhew3.mediamogul.tv.TVDBMatchStatus;
import com.mayhew3.mediamogul.xml.NodeReader;
import com.mayhew3.mediamogul.xml.NodeReaderImpl;
import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;

@SuppressWarnings("FieldCanBeLocal")
public class TVDBXMLFetcher {

  private String singleSeriesTitle = "Inside Amy Schumer"; // update for testing on a single series
  private String filePath = "src\\test\\resources\\TVDB_Inside_Amy_Schumer.xml";

  private SQLConnection connection;

  private static Logger logger = LogManager.getLogger(TVDBXMLFetcher.class);

  private TVDBXMLFetcher(SQLConnection connection) {
    this.connection = connection;
  }

  public static void main(String... args) throws URISyntaxException, SQLException, IOException, SAXException, MissingEnvException {
    ArgumentChecker argumentChecker = new ArgumentChecker(args);

    SQLConnection connection = PostgresConnectionFactory.createConnection(argumentChecker);
    TVDBXMLFetcher tvdbxmlFetcher = new TVDBXMLFetcher(connection);

    tvdbxmlFetcher.downloadXMLForSeries();
  }

  private void downloadXMLForSeries() throws SQLException, IOException, SAXException, MissingEnvException {

    String sql = "select *\n" +
        "from series\n" +
        "where tvdb_match_status = ? " +
        "and title = ? " +
        "and retired = ? ";
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, TVDBMatchStatus.MATCH_COMPLETED, singleSeriesTitle, 0);

    logger.info("Starting update.");


    if (resultSet.next()) {
      Series series = new Series();

      series.initializeFromDBObject(resultSet);

      NodeReader nodeReader = new NodeReaderImpl(filePath);

      Integer tvdbID = series.tvdbSeriesExtId.getValue();

      String apiKey = EnvironmentChecker.getOrThrow("TVDB_API_KEY");
      String url = "http://thetvdb.com/api/" + apiKey + "/series/" + tvdbID + "/all/en.xml";

      nodeReader.readXMLFromUrl(url);
    } else {
      throw new IllegalStateException("Series not found: " + singleSeriesTitle);
    }
  }


  private void debug(Object message) {
    logger.debug(message);
  }

}

