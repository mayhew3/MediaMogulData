package com.mayhew3.mediamogul.scheduler;

import com.mashape.unirest.http.exceptions.UnirestException;
import com.mayhew3.mediamogul.ChromeProvider;
import com.mayhew3.mediamogul.EnvironmentChecker;
import com.mayhew3.mediamogul.ExternalServiceHandler;
import com.mayhew3.mediamogul.ExternalServiceType;
import com.mayhew3.mediamogul.archive.OldDataArchiveRunner;
import com.mayhew3.mediamogul.backup.MediaMogulBackupExecutor;
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

import java.io.PrintStream;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class TaskScheduleRunner {
  private List<PeriodicTaskSchedule> taskSchedules = new ArrayList<>();

  private SQLConnection connection;

  @Nullable
  private TVDBJWTProvider tvdbjwtProvider;
  private JSONReader jsonReader;
  private ExternalServiceHandler howLongServiceHandler;
  private IGDBProvider igdbProvider;
  private SteamProvider steamProvider;
  private ChromeProvider chromeProvider;

  private String envName;

  private Integer person_id;

  private static Logger logger = LogManager.getLogger(TaskScheduleRunner.class);

  private TaskScheduleRunner(SQLConnection connection,
                             @Nullable TVDBJWTProvider tvdbjwtProvider,
                             JSONReader jsonReader,
                             ExternalServiceHandler howLongServiceHandler, IGDBProvider igdbProvider, SteamProvider steamProvider, ChromeProvider chromeProvider, String envName, Integer person_id) {
    this.connection = connection;
    this.tvdbjwtProvider = tvdbjwtProvider;
    this.jsonReader = jsonReader;
    this.howLongServiceHandler = howLongServiceHandler;
    this.igdbProvider = igdbProvider;
    this.steamProvider = steamProvider;
    this.chromeProvider = chromeProvider;
    this.envName = envName;
    this.person_id = person_id;
  }

  public static void main(String... args) throws URISyntaxException, SQLException, MissingEnvException {
    String databaseUrl = EnvironmentChecker.getOrThrow("DATABASE_URL");

    SQLConnection connection = PostgresConnectionFactory.initiateDBConnect(databaseUrl);
    JSONReader jsonReader = new JSONReaderImpl();
    ExternalServiceHandler tvdbServiceHandler = new ExternalServiceHandler(connection, ExternalServiceType.TVDB);
    ExternalServiceHandler howLongServiceHandler = new ExternalServiceHandler(connection, ExternalServiceType.HowLongToBeat);
    IGDBProviderImpl igdbProvider = new IGDBProviderImpl();
    String mediaMogulPersonID = EnvironmentChecker.getOrThrow("MediaMogulPersonID");
    Integer person_id = Integer.parseInt(mediaMogulPersonID);

    String envName = EnvironmentChecker.getOrThrow("envName");

    ChromeProvider chromeProvider = new ChromeProvider();

    TVDBJWTProvider tvdbjwtProvider = null;
    try {
      tvdbjwtProvider = new TVDBJWTProviderImpl(tvdbServiceHandler);
    } catch (UnirestException e) {
      e.printStackTrace();
    }

    TaskScheduleRunner taskScheduleRunner = new TaskScheduleRunner(
        connection,
        tvdbjwtProvider,
        jsonReader,
        howLongServiceHandler,
        igdbProvider,
        new SteamProviderImpl(),
        chromeProvider,
        envName,
        person_id);
    taskScheduleRunner.runUpdates();
  }

  private void createLocalTaskList() throws MissingEnvException {
    String backupEnv = EnvironmentChecker.getOrThrow("backupEnv");
    addHourlyTask(new OldDataArchiveRunner(connection, backupEnv), 1);
    addHourlyTask(new MediaMogulBackupExecutor(backupEnv), 24);
  }

  private void redirectOutputToLogger() {
    System.setOut(createLoggingProxy(System.out));
    System.setErr(createLoggingProxy(System.err));
  }

  public static PrintStream createLoggingProxy(final PrintStream realPrintStream) {
    return new PrintStream(realPrintStream) {
      public void print(final String string) {
        realPrintStream.print(string);
        logger.info(string);
      }
    };
  }

  private void createTaskList() throws MissingEnvException {

    // MINUTELY

    addMinutelyTask(new NewSeriesChecker(connection, tvdbjwtProvider, jsonReader),
        1);
    addMinutelyTask(new NewGameChecker(connection, jsonReader, igdbProvider, chromeProvider, howLongServiceHandler, person_id),
        1);
    addMinutelyTask(new TVDBUpdateRunner(connection, tvdbjwtProvider, jsonReader, UpdateMode.MANUAL),
        1);
    addMinutelyTask(new SeriesDenormUpdater(connection),
        30);
    addMinutelyTask(new TVDBUpdateProcessor(connection, tvdbjwtProvider, jsonReader),
        1);
    addMinutelyTask(new TVDBUpdateFinder(connection, tvdbjwtProvider, jsonReader),
        2);
    addMinutelyTask(new SteamPlaySessionGenerator(connection, person_id),
        10);
    addMinutelyTask(new TVDBUpdateRunner(connection, tvdbjwtProvider, jsonReader, UpdateMode.SANITY),
        4);
    addMinutelyTask(new IGDBUpdateRunner(connection, igdbProvider, jsonReader, UpdateMode.SMART),
        30);
    addMinutelyTask(new TVDBUpdateRunner(connection, tvdbjwtProvider, jsonReader, UpdateMode.SMART),
        30);
    addMinutelyTask(new MetacriticTVUpdateRunner(connection, UpdateMode.CHUNKED),
        7);
    addMinutelyTask(new MetacriticTVUpdateRunner(connection, UpdateMode.SANITY),
        14);
    addMinutelyTask(new HowLongToBeatUpdateRunner(connection, UpdateMode.QUICK, howLongServiceHandler, chromeProvider),
        30);

    // HOURLY
    addHourlyTask(new SteamGameUpdateRunner(connection, person_id, steamProvider, chromeProvider),
        1);
    addHourlyTask(new HowLongToBeatUpdateRunner(connection, UpdateMode.PING, howLongServiceHandler, chromeProvider),
        1);
    addHourlyTask(new IGDBUpdateRunner(connection, igdbProvider, jsonReader, UpdateMode.SANITY),
        24);
    addHourlyTask(new MetacriticGameUpdateRunner(connection, UpdateMode.UNMATCHED, person_id),
        24);
    addHourlyTask(new EpisodeGroupUpdater(connection),
        24);
    addHourlyTask(new SteamAttributeUpdateRunner(connection, UpdateMode.FULL, chromeProvider),
        24);
    addHourlyTask(new GiantBombUpdateRunner(connection),
        24);
    addHourlyTask(new CloudinaryUploadRunner(connection, UpdateMode.FULL),
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

  private void runUpdates() throws MissingEnvException {
    if (tvdbjwtProvider == null) {
      throw new IllegalStateException("Can't currently run updater with no TVDB token. TVDB is the only thing it can handle yet.");
    }

    if (isRunningOnHeroku()) {
      createTaskList();
    } else {
      redirectOutputToLogger();
      createLocalTaskList();
    }

    info("");
    info("SESSION START!");
    info("");

    runEligibleTasks();

  }

  private boolean isRunningOnHeroku() {
    return "Heroku".equals(envName);
  }

  private void scheduleNextFutureTask() {
    PeriodicTaskSchedule nextTask = getNextTask();
    Long millisUntilNextRun = nextTask.getMillisUntilNextRun();

    long secondsUntilNextRun = millisUntilNextRun / 1000;
    long minutesUntilNextRun = millisUntilNextRun / 1000 / 60;

    long remainderSeconds = secondsUntilNextRun - (minutesUntilNextRun * 60);

    logger.debug("Scheduling next task '" + nextTask + "' to run in " + minutesUntilNextRun + " min " + remainderSeconds +
        " sec.");

    DelayedTask delayedTask = new DelayedTask(this, nextTask);

    if (millisUntilNextRun < 0) {
      millisUntilNextRun = 1L;
    }

    if (minutesUntilNextRun > 5) {
      try {
        logger.info("Next task is " + minutesUntilNextRun + " minutes away. Closing DB connection temporarily.");
        connection.closeConnection();
      } catch (SQLException e) {
        logger.info("Failed to close connection: " + e.getLocalizedMessage());
      }
    }

    Timer timer = new Timer();
    timer.schedule(delayedTask, millisUntilNextRun);
  }

  private class DelayedTask extends TimerTask {

    private TaskScheduleRunner runner;
    private PeriodicTaskSchedule nextTask;

    private DelayedTask(TaskScheduleRunner runner, PeriodicTaskSchedule nextTask) {
      this.runner = runner;
      this.nextTask = nextTask;
    }

    @Override
    public void run() {
      logger.debug("Timer complete! Beginning delayed task: " + this.nextTask);
      runUpdateForSingleTask(this.nextTask);
      logger.debug("Finished delayed task. Throwing back to find eligible tasks.");
      runner.runEligibleTasks();
    }
  }

  private void runEligibleTasks() {

    List<PeriodicTaskSchedule> eligibleTasks = taskSchedules.stream()
        .filter(PeriodicTaskSchedule::isEligibleToRun)
        .collect(Collectors.toList());

    if (eligibleTasks.isEmpty()) {
      logger.debug("No eligible tasks. Scheduling next task.");
      scheduleNextFutureTask();
    } else {
      updateTasks(eligibleTasks);
    }
  }

  private void updateTasks(List<PeriodicTaskSchedule> eligibleTasks) {
    logger.debug("Found " + eligibleTasks.size() + " tasks to run.");

    for (PeriodicTaskSchedule taskSchedule : eligibleTasks) {
      runUpdateForSingleTask(taskSchedule);
    }

    logger.debug("Finished current loop. Finding more eligible tasks.");
    runEligibleTasks();
  }

  private void runUpdateForSingleTask(PeriodicTaskSchedule taskSchedule) {
    UpdateRunner updateRunner = taskSchedule.getUpdateRunner();
    try {
      ConnectionLogger connectionLogger = new ConnectionLogger(connection);

      logger.debug("Starting update for '" + updateRunner.getUniqueIdentifier() + "'");

      connectionLogger.logConnectionStart(updateRunner);
      updateRunner.runUpdate();
      connectionLogger.logConnectionEnd();

      logger.debug("Update complete for '" + updateRunner.getUniqueIdentifier() + "'");

    } catch (Exception e) {
      logger.error("Exception encountered during run of update '" + updateRunner.getUniqueIdentifier() + "'.");
      e.printStackTrace();
    } finally {
      // mark the task as having been run, whether it succeeds or errors out.
      taskSchedule.updateLastRanToNow();
    }
  }

  private PeriodicTaskSchedule getNextTask() {
    Optional<PeriodicTaskSchedule> taskSchedule = taskSchedules.stream()
        .min(Comparator.comparing(PeriodicTaskSchedule::getMillisUntilNextRun));
    if (taskSchedule.isEmpty()) {
      throw new RuntimeException("No tasks found!");
    }
    return taskSchedule.get();
  }

  private static void info(Object message) {
    logger.info(message);
  }

}
