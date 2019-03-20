package com.mayhew3.mediamogul;

import com.google.common.collect.Lists;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.mediamogul.games.SteamGameUpdater;
import com.mayhew3.mediamogul.games.provider.SteamProviderImpl;
import com.mayhew3.mediamogul.scheduler.UpdateRunner;
import com.mayhew3.mediamogul.tv.*;
import com.mayhew3.mediamogul.tv.helper.ConnectionLogger;
import com.mayhew3.mediamogul.tv.helper.UpdateMode;
import com.mayhew3.mediamogul.tv.provider.RemoteFileDownloader;
import com.mayhew3.mediamogul.tv.provider.TVDBJWTProviderImpl;
import com.mayhew3.mediamogul.xml.JSONReaderImpl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class QuickBatchUpdater {

  public static void main(String... args) throws FileNotFoundException, URISyntaxException, SQLException {
    List<String> argList = Lists.newArrayList(args);
    Boolean logToFile = argList.contains("LogToFile");
    Boolean saveTiVoXML = argList.contains("SaveTiVoXML");

    ArgumentChecker argumentChecker = new ArgumentChecker(args);
    String identifier = argumentChecker.getDBIdentifier();

    if (logToFile) {
      SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
      String dateFormatted = simpleDateFormat.format(new Date());

      String mediaMogulLogs = System.getenv("MediaMogulLogs");

      File file = new File(mediaMogulLogs + "\\QuickBatchUpdater_" + dateFormatted + "_" + identifier + ".log");
      FileOutputStream fos = new FileOutputStream(file, true);
      PrintStream ps = new PrintStream(fos);
      System.setErr(ps);
      System.setOut(ps);
    }

    debug("");
    debug("SESSION START!");
    debug("");

    SQLConnection connection = PostgresConnectionFactory.createConnection(argumentChecker);
    Integer person_id = Integer.parseInt(System.getenv("MediaMogulPersonID"));

    // INITIALIZE UPDATERS

    List<UpdateRunner> updateRunners = Lists.newArrayList(
        new TiVoCommunicator(connection, new RemoteFileDownloader(saveTiVoXML), UpdateMode.QUICK),
        new SteamGameUpdater(connection, person_id, new SteamProviderImpl())
    );

    try {
      ExternalServiceHandler tvdbServiceHandler = new ExternalServiceHandler(connection, ExternalServiceType.TVDB);
      TVDBJWTProviderImpl tvdbjwtProvider = new TVDBJWTProviderImpl(tvdbServiceHandler);
      updateRunners.add(new TVDBUpdateRunner(connection, tvdbjwtProvider, new JSONReaderImpl(), UpdateMode.SMART));
      updateRunners.add(new TVDBSeriesMatchRunner(connection, tvdbjwtProvider, new JSONReaderImpl(), UpdateMode.SMART));
    } catch (UnirestException e) {
      debug("Error getting TVDB token. Skipping TVDB updates.");
      e.printStackTrace();
    }

    updateRunners.add(new SeriesDenormUpdater(connection));


    // RUN UPDATERS

    ConnectionLogger logger = new ConnectionLogger(connection);

    logger.logConnectionStart(false);

    for (UpdateRunner updateRunner : updateRunners) {
      String runnerName = updateRunner.getUniqueIdentifier();
      debug("Beginning execution of updater '" + runnerName + "'");

      try {
        updateRunner.runUpdate();
        debug("Finished execution of updater '" + runnerName + "'");
      } catch (Exception e) {
        debug("Error during update: " + runnerName);
        e.printStackTrace();
      }
    }

    logger.logConnectionEnd();

    connection.closeConnection();
  }

  protected static void debug(Object object) {
    System.out.println(new Date() + ": " + object);
  }

}
