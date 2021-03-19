package com.mayhew3.mediamogul.tv;

import com.google.common.collect.Lists;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.mayhew3.mediamogul.ExternalServiceHandler;
import com.mayhew3.mediamogul.ExternalServiceType;
import com.mayhew3.mediamogul.db.DatabaseEnvironments;
import com.mayhew3.mediamogul.socket.MySocketFactory;
import com.mayhew3.mediamogul.socket.SocketWrapper;
import com.mayhew3.mediamogul.tv.helper.ConnectionLogger;
import com.mayhew3.mediamogul.tv.helper.UpdateMode;
import com.mayhew3.mediamogul.tv.provider.RemoteFileDownloader;
import com.mayhew3.mediamogul.tv.provider.TVDBJWTProviderImpl;
import com.mayhew3.mediamogul.xml.BadlyFormattedXMLException;
import com.mayhew3.mediamogul.xml.JSONReaderImpl;
import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.EnvironmentChecker;
import com.mayhew3.postgresobject.db.DatabaseEnvironment;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
import com.mayhew3.postgresobject.exception.MissingEnvException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class TiVoLibraryUpdater {

  private static Logger logger = LogManager.getLogger(TiVoLibraryUpdater.class);

  public static void main(String... args) throws FileNotFoundException, URISyntaxException, SQLException, MissingEnvException {
    List<String> argList = Lists.newArrayList(args);
    Boolean nightly = argList.contains("FullMode");
    Boolean tvdbOnly = argList.contains("TVDBOnly");
    Boolean tiVoOnly = argList.contains("TiVoOnly");
    Boolean logToFile = argList.contains("LogToFile");
    Boolean saveTiVoXML = argList.contains("SaveTiVoXML");

    ArgumentChecker argumentChecker = new ArgumentChecker(args);
    UpdateMode updateType = nightly ? UpdateMode.FULL : UpdateMode.QUICK;

    argumentChecker.addExpectedOption("socketEnv", true, "Socket environment to connect to.");

    String socketEnv = argumentChecker.getRequiredValue("socketEnv");

    String appRole = argumentChecker.getRequiredValue("appRole");

    SocketWrapper socket = new MySocketFactory().createSocket(socketEnv, appRole);

    if (logToFile) {
      SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
      String dateFormatted = simpleDateFormat.format(new Date());

      String mediaMogulLogs = EnvironmentChecker.getOrThrow("MediaMogulLogs");

      File file = new File(mediaMogulLogs + "\\TiVoUpdaterPostgres_" + dateFormatted + "_" + argumentChecker.getDBIdentifier() + ".log");
      FileOutputStream fos = new FileOutputStream(file, true);
      PrintStream ps = new PrintStream(fos);
      System.setErr(ps);
      System.setOut(ps);
    }

    debug("");
    debug("SESSION START! Date: " + new Date());
    debug("");

    DatabaseEnvironment environment = DatabaseEnvironments.getEnvironmentForDBArgument(argumentChecker);
    SQLConnection connection = PostgresConnectionFactory.createConnection(environment);

    ConnectionLogger logger = new ConnectionLogger(connection);

//    logger.logConnectionStart(nightly);

    if (!tvdbOnly) {
      try {
        TiVoCommunicator tiVoCommunicator = new TiVoCommunicator(connection, new RemoteFileDownloader(saveTiVoXML), updateType);
        tiVoCommunicator.runUpdate();
      } catch (BadlyFormattedXMLException e) {
        debug("Error parsing TiVo XML.");
        e.printStackTrace();
      } catch (SQLException e) {
        debug("SQL error during TiVo update.");
        e.printStackTrace();
      }
    }

    if (!tiVoOnly) {
      try {
        ExternalServiceHandler tvdbServiceHandler = new ExternalServiceHandler(connection, ExternalServiceType.TVDB);
        TVDBUpdateRunner tvdbUpdateRunner = new TVDBUpdateRunner(connection, new TVDBJWTProviderImpl(tvdbServiceHandler), new JSONReaderImpl(), socket, UpdateMode.SMART);
        tvdbUpdateRunner.runUpdate();
      } catch (SQLException e) {
        debug("Error downloading info from TVDB service.");
        e.printStackTrace();
      } catch (UnirestException e) {
        debug("Error initiating TVDB credentials.");
        e.printStackTrace();
      }
    }

    if (!tiVoOnly) {
      try {
        ExternalServiceHandler tvdbServiceHandler = new ExternalServiceHandler(connection, ExternalServiceType.TVDB);
        TVDBSeriesMatchRunner tvdbUpdateRunner = new TVDBSeriesMatchRunner(connection, new TVDBJWTProviderImpl(tvdbServiceHandler), new JSONReaderImpl(), UpdateMode.SMART);
        tvdbUpdateRunner.runUpdate();
      } catch (SQLException e) {
        debug("Error trying to match series with TVDB.");
        e.printStackTrace();
      } catch (UnirestException e) {
        debug("Error initiating TVDB credentials.");
        e.printStackTrace();
      }
    }

    if (nightly) {
      try {
        MetacriticTVUpdateRunner metacriticTVUpdateRunner = new MetacriticTVUpdateRunner(connection, UpdateMode.FULL);
        metacriticTVUpdateRunner.runFullUpdate();
      } catch (Exception e) {
        debug("Uncaught exception during metacritic update.");
        e.printStackTrace();
      }
    }
    
    // update any shows that haven't been run in a while
    if (nightly) {
      try {
        ExternalServiceHandler tvdbServiceHandler = new ExternalServiceHandler(connection, ExternalServiceType.TVDB);
        TVDBUpdateRunner tvdbUpdateRunner = new TVDBUpdateRunner(connection, new TVDBJWTProviderImpl(tvdbServiceHandler), new JSONReaderImpl(), socket, UpdateMode.SANITY);
        tvdbUpdateRunner.runUpdate();
      } catch (UnirestException e) {
        debug("Uncaught exception during TVDB sanity check.");
        e.printStackTrace();
      }
    }

    if (nightly) {
      try {
        debug("Updating EpisodeGroupRatings...");
        EpisodeGroupUpdater episodeGroupUpdater = new EpisodeGroupUpdater(connection, null);
        episodeGroupUpdater.runUpdate();
      } catch (Exception e) {
        debug("Uncaught exception during episode group rating update.");
        e.printStackTrace();
      }
    }

    try {
      debug("Updating denorms...");
      SeriesDenormUpdater denormUpdater = new SeriesDenormUpdater(connection);
      denormUpdater.runUpdate();
      debug("Denorms updated.");
    } catch (Exception e) {
      debug("Error updating series denorms.");
      e.printStackTrace();
    }

    logger.logConnectionEnd();

    connection.closeConnection();
  }

  private static void debug(Object message) {
    logger.debug(message);
  }

}
