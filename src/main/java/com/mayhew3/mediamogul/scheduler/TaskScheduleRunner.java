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
import org.jetbrains.annotations.Nullable;

import java.net.URISyntaxException;
import java.sql.SQLException;
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
  ExternalServiceHandler howLongServiceHandler;
  private IGDBProvider igdbProvider;
  private SteamProvider steamProvider;
  private Integer person_id;

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
    // REGULAR

    addPeriodicTask(new SeriesDenormUpdater(connection),
        5);
    addPeriodicTask(new TVDBUpdateRunner(connection, tvdbjwtProvider, jsonReader, UpdateMode.MANUAL),
        1);
    addPeriodicTask(new TVDBUpdateFinder(connection, tvdbjwtProvider, jsonReader),
        2);
    addPeriodicTask(new TVDBUpdateProcessor(connection, tvdbjwtProvider, jsonReader),
        1);
    addPeriodicTask(new TVDBSeriesMatchRunner(connection, tvdbjwtProvider, jsonReader, UpdateMode.SMART),
        3);

    addPeriodicTask(new IGDBUpdateRunner(connection, igdbProvider, jsonReader, UpdateMode.SMART),
        5);
    addPeriodicTask(new SteamPlaySessionGenerator(connection, person_id),
        10);
    addPeriodicTask(new TVDBUpdateRunner(connection, tvdbjwtProvider, jsonReader, UpdateMode.SMART),
        30);
    addPeriodicTask(new SteamGameUpdater(connection, person_id, steamProvider),
        60);
    addPeriodicTask(new CloudinaryUploader(connection, UpdateMode.QUICK),
        60);

    // NIGHTLY
    addNightlyTask(new IGDBUpdateRunner(connection, igdbProvider, jsonReader, UpdateMode.SANITY));
    addNightlyTask(new MetacriticTVUpdater(connection, UpdateMode.FULL));
    addNightlyTask(new MetacriticGameUpdateRunner(connection, UpdateMode.UNMATCHED));
    addNightlyTask(new TVDBUpdateRunner(connection, tvdbjwtProvider, jsonReader, UpdateMode.SANITY));
    addNightlyTask(new EpisodeGroupUpdater(connection));
    addNightlyTask(new SteamAttributeUpdateRunner(connection, UpdateMode.FULL));
    addNightlyTask(new HowLongToBeatUpdateRunner(connection, UpdateMode.QUICK, howLongServiceHandler));
    addNightlyTask(new GiantBombUpdater(connection));
    addNightlyTask(new CloudinaryUploader(connection, UpdateMode.FULL));
  }

  private void addPeriodicTask(UpdateRunner updateRunner, Integer minutesBetween) {
    taskSchedules.add(new PeriodicTaskSchedule(updateRunner, connection, minutesBetween));
  }

  private void addNightlyTask(UpdateRunner updateRunner) {
    taskSchedules.add(new NightlyTaskSchedule(updateRunner, connection, 1));
  }

  @SuppressWarnings("InfiniteLoopStatement")
  private void runUpdates() throws InterruptedException, MissingEnvException {
    if (tvdbjwtProvider == null) {
      throw new IllegalStateException("Can't currently run updater with no TVDB token. TVDB is the only thing it can handle yet.");
    }

    createTaskList();

    debug("");
    debug("SESSION START!");
    debug("");

    while (true) {

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

  protected static void debug(Object message) {
    System.out.println(new Date() + ": " + message);
  }

}
