package com.mayhew3.mediamogul.scheduler;

import com.google.common.collect.Lists;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.mayhew3.mediamogul.ExternalServiceHandler;
import com.mayhew3.mediamogul.ExternalServiceType;
import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.mediamogul.archive.OldDataArchiveRunner;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
import com.mayhew3.mediamogul.games.*;
import com.mayhew3.mediamogul.games.provider.IGDBProvider;
import com.mayhew3.mediamogul.games.provider.IGDBProviderImpl;
import com.mayhew3.mediamogul.games.provider.SteamProvider;
import com.mayhew3.mediamogul.games.provider.SteamProviderImpl;
import com.mayhew3.mediamogul.tv.*;
import com.mayhew3.mediamogul.tv.helper.ConnectionLogger;
import com.mayhew3.mediamogul.tv.helper.UpdateMode;
import com.mayhew3.mediamogul.tv.provider.RemoteFileDownloader;
import com.mayhew3.mediamogul.tv.provider.TVDBJWTProvider;
import com.mayhew3.mediamogul.tv.provider.TVDBJWTProviderImpl;
import com.mayhew3.mediamogul.tv.provider.TiVoDataProvider;
import com.mayhew3.mediamogul.xml.JSONReader;
import com.mayhew3.mediamogul.xml.JSONReaderImpl;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.Thread.sleep;

public class TaskScheduleRunner {
  private List<TaskSchedule> taskSchedules = new ArrayList<>();

  private SQLConnection connection;

  @Nullable
  private TVDBJWTProvider tvdbjwtProvider;
  private JSONReader jsonReader;
  private TiVoDataProvider tiVoDataProvider;
  private IGDBProvider igdbProvider;
  private SteamProvider steamProvider;
  ExternalServiceHandler howLongServiceHandler;

  private String identifier;

  private PrintStream originalStream = System.out;
  private Integer person_id;

  private Boolean logToFile;
  private PrintStream logOutput = null;

  private TaskScheduleRunner(SQLConnection connection, @Nullable TVDBJWTProvider tvdbjwtProvider, JSONReader jsonReader, TiVoDataProvider tiVoDataProvider, IGDBProvider igdbProvider, String identifier, Integer person_id, Boolean logToFile, SteamProviderImpl steamProvider, ExternalServiceHandler howLongServiceHandler) {
    this.connection = connection;
    this.tvdbjwtProvider = tvdbjwtProvider;
    this.jsonReader = jsonReader;
    this.tiVoDataProvider = tiVoDataProvider;
    this.igdbProvider = igdbProvider;
    this.identifier = identifier;
    this.person_id = person_id;
    this.logToFile = logToFile;
    this.steamProvider = steamProvider;
    this.howLongServiceHandler = howLongServiceHandler;
  }

  public static void main(String... args) throws URISyntaxException, SQLException, FileNotFoundException, InterruptedException {
    List<String> argList = Lists.newArrayList(args);
    Boolean logToFile = argList.contains("LogToFile");

    ArgumentChecker argumentChecker = new ArgumentChecker(args);

    SQLConnection connection = PostgresConnectionFactory.createConnection(argumentChecker);
    JSONReader jsonReader = new JSONReaderImpl();
    TiVoDataProvider tiVoDataProvider = new RemoteFileDownloader(false);
    IGDBProviderImpl igdbProvider = new IGDBProviderImpl();
    ExternalServiceHandler tvdbServiceHandler = new ExternalServiceHandler(connection, ExternalServiceType.TVDB);
    ExternalServiceHandler howLongServiceHandler = new ExternalServiceHandler(connection, ExternalServiceType.HowLongToBeat);

    TVDBJWTProvider tvdbjwtProvider = null;
    try {
      tvdbjwtProvider = new TVDBJWTProviderImpl(tvdbServiceHandler);
    } catch (UnirestException e) {
      e.printStackTrace();
    }

    setDriverPath();

    Integer person_id = Integer.parseInt(System.getenv("MediaMogulPersonID"));
    TaskScheduleRunner taskScheduleRunner = new TaskScheduleRunner(
        connection,
        tvdbjwtProvider,
        jsonReader,
        tiVoDataProvider,
        igdbProvider,
        argumentChecker.getDBIdentifier(),
        person_id,
        logToFile,
        new SteamProviderImpl(),
        howLongServiceHandler);
    taskScheduleRunner.runUpdates();
  }

  private void createTaskList() {
    // REGULAR
    addPeriodicTask(new OldDataArchiveRunner(connection),
        30);

    // NIGHTLY
    addNightlyTask(new TiVoCommunicator(connection, tiVoDataProvider, UpdateMode.FULL));
    addNightlyTask(new MetacriticGameUpdateRunner(connection, UpdateMode.UNMATCHED));
    addNightlyTask(new SteamAttributeUpdateRunner(connection, UpdateMode.FULL));
    addNightlyTask(new HowLongToBeatUpdateRunner(connection, UpdateMode.QUICK, howLongServiceHandler));
  }

  private void addPeriodicTask(UpdateRunner updateRunner, Integer minutesBetween) {
    taskSchedules.add(new PeriodicTaskSchedule(updateRunner, connection, minutesBetween));
  }

  private void addNightlyTask(UpdateRunner updateRunner) {
    taskSchedules.add(new NightlyTaskSchedule(updateRunner, connection, 1));
  }

  @SuppressWarnings("InfiniteLoopStatement")
  private void runUpdates() throws FileNotFoundException, InterruptedException {
    if (tvdbjwtProvider == null) {
      throw new IllegalStateException("Can't currently run updater with no TVDB token. TVDB is the only thing it can handle yet.");
    }

    if (logToFile) {
      openLogStream();
    }

    createTaskList();

    debug("");
    debug("SESSION START!");
    debug("");

    while (true) {
      if (logToFile && logOutput == null) {
        openLogStream();
      }

      List<TaskSchedule> eligibleTasks = taskSchedules.stream()
          .filter(TaskSchedule::isEligibleToRun)
          .collect(Collectors.toList());

      for (TaskSchedule taskSchedule : eligibleTasks) {
        UpdateRunner updateRunner = taskSchedule.getUpdateRunner();
        try {
          ConnectionLogger connectionLogger = new ConnectionLogger(connection);

          debug("Starting update for '" + updateRunner.getUniqueIdentifier() + "'");

          connectionLogger.logConnectionStart(updateRunner);
          updateRunner.runUpdate();
          connectionLogger.logConnectionEnd();

          debug("Update complete for '" + updateRunner.getUniqueIdentifier() + "'");

        } catch (Exception e) {
          debug("Exception encountered during run of update '" + updateRunner.getUniqueIdentifier() + "'.");
          e.printStackTrace();
        } finally {
          // mark the task as having been run, whether it succeeds or errors out.
          taskSchedule.updateLastRanToNow();
        }
      }

      if (logToFile && logOutput != null) {
        closeLogStream();
      }

      sleep(15000);
    }
  }


  private void openLogStream() throws FileNotFoundException {
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
    String dateFormatted = simpleDateFormat.format(new Date());

    String mediaMogulLogs = System.getenv("MediaMogulLogs");

    File file = new File(mediaMogulLogs + "\\TaskScheduleRunner_" + dateFormatted + "_" + identifier + ".log");
    FileOutputStream fos = new FileOutputStream(file, true);
    logOutput = new PrintStream(fos);

    System.setErr(logOutput);
    System.setOut(logOutput);
  }

  private void closeLogStream() {
    System.setErr(originalStream);
    System.setOut(originalStream);

    logOutput.close();
    logOutput = null;
  }

  private static void setDriverPath() {
    String driverPath = System.getProperty("user.dir") + "\\resources\\chromedriver.exe";
    System.setProperty("webdriver.chrome.driver", driverPath);
  }

  protected static void debug(Object message) {
    System.out.println(new Date() + ": " + message);
  }

}
