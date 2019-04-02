package com.mayhew3.mediamogul.scheduler;

import com.mashape.unirest.http.exceptions.UnirestException;
import com.mayhew3.mediamogul.EnvironmentChecker;
import com.mayhew3.mediamogul.ExternalServiceHandler;
import com.mayhew3.mediamogul.ExternalServiceType;
import com.mayhew3.mediamogul.exception.MissingEnvException;
import com.mayhew3.mediamogul.games.*;
import com.mayhew3.mediamogul.games.provider.IGDBProvider;
import com.mayhew3.mediamogul.games.provider.IGDBProviderImpl;
import com.mayhew3.mediamogul.games.provider.SteamProvider;
import com.mayhew3.mediamogul.games.provider.SteamProviderImpl;
import com.mayhew3.mediamogul.tv.*;
import com.mayhew3.mediamogul.tv.helper.ConnectionLogger;
import com.mayhew3.mediamogul.tv.helper.UpdateMode;
import com.mayhew3.mediamogul.tv.provider.TVDBJWTProvider;
import com.mayhew3.mediamogul.tv.provider.TVDBJWTProviderImpl;
import com.mayhew3.mediamogul.xml.JSONReader;
import com.mayhew3.mediamogul.xml.JSONReaderImpl;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.Thread.sleep;

public class TaskScheduleRunner {
  private List<TaskSchedule> taskSchedules = new ArrayList<>();

  private SQLConnection connection;

  @Nullable
  private TVDBJWTProvider tvdbjwtProvider;
  private JSONReader jsonReader;
  private ExternalServiceHandler howLongServiceHandler;
  private IGDBProvider igdbProvider;
  private SteamProvider steamProvider;
  private Integer person_id;

  private static Logger logger = LogManager.getLogger(TaskScheduleRunner.class);

  private TaskScheduleRunner(SQLConnection connection,
                             @Nullable TVDBJWTProvider tvdbjwtProvider,
                             JSONReader jsonReader,
                             ExternalServiceHandler howLongServiceHandler, IGDBProvider igdbProvider, SteamProvider steamProvider, Integer person_id) {
    this.connection = connection;
    this.tvdbjwtProvider = tvdbjwtProvider;
    this.jsonReader = jsonReader;
    this.howLongServiceHandler = howLongServiceHandler;
    this.igdbProvider = igdbProvider;
    this.steamProvider = steamProvider;
    this.person_id = person_id;
  }

  public static void main(String... args) throws URISyntaxException, SQLException, InterruptedException, MissingEnvException {
    String databaseUrl = EnvironmentChecker.getOrThrow("DATABASE_URL");

    SQLConnection connection = PostgresConnectionFactory.initiateDBConnect(databaseUrl);
    JSONReader jsonReader = new JSONReaderImpl();
    ExternalServiceHandler tvdbServiceHandler = new ExternalServiceHandler(connection, ExternalServiceType.TVDB);
    ExternalServiceHandler howLongServiceHandler = new ExternalServiceHandler(connection, ExternalServiceType.HowLongToBeat);
    IGDBProviderImpl igdbProvider = new IGDBProviderImpl();
    String mediaMogulPersonID = EnvironmentChecker.getOrThrow("MediaMogulPersonID");
    Integer person_id = Integer.parseInt(mediaMogulPersonID);

    TVDBJWTProvider tvdbjwtProvider = null;
    try {
      tvdbjwtProvider = new TVDBJWTProviderImpl(tvdbServiceHandler);
    } catch (UnirestException e) {
      e.printStackTrace();
    }

    maybeSetDriverPath();

    TaskScheduleRunner taskScheduleRunner = new TaskScheduleRunner(
        connection,
        tvdbjwtProvider,
        jsonReader,
        howLongServiceHandler,
        igdbProvider,
        new SteamProviderImpl(),
        person_id);
    taskScheduleRunner.runUpdates();
  }

  private void createTaskList() throws MissingEnvException {

    // MINUTELY

    addMinutelyTask(new SeriesDenormUpdater(connection),
        5);
    addMinutelyTask(new TVDBUpdateRunner(connection, tvdbjwtProvider, jsonReader, UpdateMode.MANUAL),
        1);
    addMinutelyTask(new TVDBUpdateFinder(connection, tvdbjwtProvider, jsonReader),
        2);
    addMinutelyTask(new TVDBUpdateProcessor(connection, tvdbjwtProvider, jsonReader),
        1);
    addMinutelyTask(new TVDBSeriesMatchRunner(connection, tvdbjwtProvider, jsonReader, UpdateMode.SMART),
        3);
    addMinutelyTask(new MetacriticTVUpdater(connection, UpdateMode.QUICK),
        2);
    addMinutelyTask(new IGDBUpdateRunner(connection, igdbProvider, jsonReader, UpdateMode.SMART),
        5);
    addMinutelyTask(new SteamPlaySessionGenerator(connection, person_id),
        10);
    addMinutelyTask(new TVDBUpdateRunner(connection, tvdbjwtProvider, jsonReader, UpdateMode.SMART),
        30);


    // HOURLY

    addHourlyTask(new SteamGameUpdater(connection, person_id, steamProvider),
        1);
    addHourlyTask(new CloudinaryUploader(connection, UpdateMode.QUICK),
        1);

    addHourlyTask(new MetacriticTVUpdater(connection, UpdateMode.SANITY),
        24);
    addHourlyTask(new IGDBUpdateRunner(connection, igdbProvider, jsonReader, UpdateMode.SANITY),
        24);
    addHourlyTask(new MetacriticTVUpdater(connection, UpdateMode.FULL),
        24);
    addHourlyTask(new MetacriticGameUpdateRunner(connection, UpdateMode.UNMATCHED),
        24);
    addHourlyTask(new TVDBUpdateRunner(connection, tvdbjwtProvider, jsonReader, UpdateMode.SANITY),
        24);
    addHourlyTask(new EpisodeGroupUpdater(connection),
        24);
    addHourlyTask(new SteamAttributeUpdateRunner(connection, UpdateMode.FULL),
        24);
    addHourlyTask(new HowLongToBeatUpdateRunner(connection, UpdateMode.QUICK, howLongServiceHandler),
        24);
    addHourlyTask(new GiantBombUpdater(connection),
        24);
    addHourlyTask(new CloudinaryUploader(connection, UpdateMode.FULL),
        24);

  }

  private void addMinutelyTask(UpdateRunner updateRunner, Integer minutesBetween) {
    taskSchedules.add(new PeriodicTaskSchedule(updateRunner, connection)
        .withMinutesBetween(minutesBetween));
  }

  private void addHourlyTask(UpdateRunner updateRunner, Integer hoursBetween) {
    taskSchedules.add(new PeriodicTaskSchedule(updateRunner, connection)
        .withHoursBetween(hoursBetween));
  }

  @SuppressWarnings("InfiniteLoopStatement")
  private void runUpdates() throws InterruptedException, MissingEnvException {
    if (tvdbjwtProvider == null) {
      throw new IllegalStateException("Can't currently run updater with no TVDB token. TVDB is the only thing it can handle yet.");
    }

    createTaskList();

    info("");
    info("SESSION START!");
    info("");

    while (true) {

      List<TaskSchedule> eligibleTasks = taskSchedules.stream()
          .filter(TaskSchedule::isEligibleToRun)
          .collect(Collectors.toList());

      for (TaskSchedule taskSchedule : eligibleTasks) {
        UpdateRunner updateRunner = taskSchedule.getUpdateRunner();
        try {
          ConnectionLogger connectionLogger = new ConnectionLogger(connection);

          info("Starting update for '" + updateRunner.getUniqueIdentifier() + "'");

          connectionLogger.logConnectionStart(updateRunner);
          updateRunner.runUpdate();
          connectionLogger.logConnectionEnd();

          info("Update complete for '" + updateRunner.getUniqueIdentifier() + "'");

        } catch (Exception e) {
          logger.error("Exception encountered during run of update '" + updateRunner.getUniqueIdentifier() + "'.");
          e.printStackTrace();
        } finally {
          // mark the task as having been run, whether it succeeds or errors out.
          taskSchedule.updateLastRanToNow();
        }
      }

      sleep(15000);
    }
  }


  private static void maybeSetDriverPath() throws MissingEnvException {
    String envName = EnvironmentChecker.getOrThrow("envName");

    if (!"Heroku".equals(envName)) {
      String driverPath = System.getProperty("user.dir") + "\\resources\\chromedriver.exe";
      System.setProperty("webdriver.chrome.driver", driverPath);
    }
  }

  private static void info(Object message) {
    logger.info(message);
  }

}
