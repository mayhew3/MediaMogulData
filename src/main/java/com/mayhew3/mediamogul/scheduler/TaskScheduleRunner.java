package com.mayhew3.mediamogul.scheduler;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.mayhew3.mediamogul.ChromeProvider;
import com.mayhew3.mediamogul.ExternalServiceHandler;
import com.mayhew3.mediamogul.ExternalServiceType;
import com.mayhew3.mediamogul.archive.OldDataArchiveRunner;
import com.mayhew3.mediamogul.backup.MediaMogulBackupExecutor;
import com.mayhew3.postgresobject.db.DatabaseEnvironment;
import com.mayhew3.mediamogul.db.DatabaseEnvironments;
import com.mayhew3.mediamogul.db.ExecutionEnvironment;
import com.mayhew3.mediamogul.db.ExecutionEnvironments;
import com.mayhew3.mediamogul.games.*;
import com.mayhew3.mediamogul.games.provider.IGDBProvider;
import com.mayhew3.mediamogul.games.provider.IGDBProviderImpl;
import com.mayhew3.mediamogul.games.provider.SteamProvider;
import com.mayhew3.mediamogul.games.provider.SteamProviderImpl;
import com.mayhew3.mediamogul.socket.MySocketFactory;
import com.mayhew3.mediamogul.socket.SocketWrapper;
import com.mayhew3.mediamogul.tv.*;
import com.mayhew3.mediamogul.tv.helper.ConnectionLogger;
import com.mayhew3.mediamogul.tv.helper.UpdateMode;
import com.mayhew3.mediamogul.tv.provider.TVDBJWTProvider;
import com.mayhew3.mediamogul.tv.provider.TVDBJWTProviderImpl;
import com.mayhew3.mediamogul.xml.JSONReader;
import com.mayhew3.mediamogul.xml.JSONReaderImpl;
import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.EnvironmentChecker;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
import com.mayhew3.postgresobject.exception.MissingEnvException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import java.io.PrintStream;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class TaskScheduleRunner {
  private final List<PeriodicTaskSchedule> taskSchedules = new ArrayList<>();

  private final SQLConnection connection;

  @Nullable
  private final TVDBJWTProvider tvdbjwtProvider;
  private final JSONReader jsonReader;
  private final ExternalServiceHandler howLongServiceHandler;
  private final IGDBProvider igdbProvider;
  private final SteamProvider steamProvider;
  private final ChromeProvider chromeProvider;


  private final SocketWrapper socket;
  private final ExecutionEnvironment executionEnvironment;
  private final DatabaseEnvironment databaseEnvironment;

  private final Integer person_id;

  private static final Logger logger = LogManager.getLogger(TaskScheduleRunner.class);

  private TaskScheduleRunner(SQLConnection connection,
                             @Nullable TVDBJWTProvider tvdbjwtProvider,
                             JSONReader jsonReader,
                             ExternalServiceHandler howLongServiceHandler,
                             IGDBProvider igdbProvider,
                             SteamProvider steamProvider,
                             ChromeProvider chromeProvider,
                             SocketWrapper socket,
                             ExecutionEnvironment executionEnvironment,
                             DatabaseEnvironment databaseEnvironment,
                             Integer person_id) {
    this.connection = connection;
    this.tvdbjwtProvider = tvdbjwtProvider;
    this.jsonReader = jsonReader;
    this.howLongServiceHandler = howLongServiceHandler;
    this.igdbProvider = igdbProvider;
    this.steamProvider = steamProvider;
    this.chromeProvider = chromeProvider;
    this.socket = socket;
    this.executionEnvironment = executionEnvironment;
    this.databaseEnvironment = databaseEnvironment;
    this.person_id = person_id;
  }

  public static void main(String... args) throws URISyntaxException, SQLException, MissingEnvException, UnirestException {
    ArgumentChecker argumentChecker = new ArgumentChecker(args);
    argumentChecker.addExpectedOption("socketEnv", true, "Socket environment to connect to.");
    argumentChecker.addExpectedOption("appRole", false, "Role to connect to socket as.");

    String socketEnv = argumentChecker.getRequiredValue("socketEnv");
    Optional<String> maybeAppRole = argumentChecker.getOptionalIdentifier("appRole");

    List<String> acceptableRoles = Lists.newArrayList("updater", "backup");

    ExecutionEnvironment executionEnvironment = getExecutionEnvironment();
    DatabaseEnvironment databaseEnvironment = getDatabaseEnvironment(argumentChecker);

    String databaseUrl = databaseEnvironment.getDatabaseUrl();
    SQLConnection connection = PostgresConnectionFactory.initiateDBConnect(databaseUrl);

    JSONReader jsonReader = new JSONReaderImpl();
    ExternalServiceHandler tvdbServiceHandler = new ExternalServiceHandler(connection, ExternalServiceType.TVDB);
    ExternalServiceHandler howLongServiceHandler = new ExternalServiceHandler(connection, ExternalServiceType.HowLongToBeat);
    IGDBProviderImpl igdbProvider = new IGDBProviderImpl();
    String mediaMogulPersonID = EnvironmentChecker.getOrThrow("MediaMogulPersonID");
    Integer person_id = Integer.parseInt(mediaMogulPersonID);

    String appRole;
    appRole = maybeAppRole.orElseGet(executionEnvironment::getAppRole);

    Preconditions.checkArgument(acceptableRoles.contains(appRole));

    SocketWrapper socket = new MySocketFactory().createSocket(socketEnv, appRole);

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
        socket,
        executionEnvironment,
        databaseEnvironment,
        person_id);
    taskScheduleRunner.runUpdates();
  }

  private void createLocalTaskList() throws MissingEnvException {
    addHourlyTask(new OldDataArchiveRunner(connection, databaseEnvironment.getEnvironmentName()), 1);
    addHourlyTask(new MediaMogulBackupExecutor(databaseEnvironment), 24);
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

  private static ExecutionEnvironment getExecutionEnvironment() throws MissingEnvException {
    String envName = EnvironmentChecker.getOrThrow("envName");
    ExecutionEnvironment executionEnvironment = ExecutionEnvironments.getThisEnvironment();
    if (executionEnvironment == null) {
      throw new IllegalArgumentException("No execution environment found matching '" + envName + "'");
    }
    return executionEnvironment;
  }

  private static DatabaseEnvironment getDatabaseEnvironment(ArgumentChecker argumentChecker) {
    String dbName = argumentChecker.getRequiredValue("db");
    DatabaseEnvironment databaseEnvironment = DatabaseEnvironments.environments.get(dbName);
    if (databaseEnvironment == null) {
      throw new IllegalArgumentException("No db found matching '" + dbName + "'");
    }
    return databaseEnvironment;
  }

  private void createTaskList() throws MissingEnvException {

    // MINUTELY

    addMinutelyTask(new NewSeriesChecker(connection, tvdbjwtProvider, jsonReader, socket),
        1);
    addMinutelyTask(new NewGameChecker(connection, jsonReader, igdbProvider, chromeProvider, howLongServiceHandler, person_id),
        1);
    addMinutelyTask(new TVDBUpdateRunner(connection, tvdbjwtProvider, jsonReader, socket, UpdateMode.MANUAL),
        1);
    addMinutelyTask(new SeriesDenormUpdater(connection),
        30);
    addMinutelyTask(new TVDBUpdateProcessor(connection, tvdbjwtProvider, jsonReader, socket),
        1);
    addMinutelyTask(new TVDBUpdateFinder(connection, tvdbjwtProvider, jsonReader),
        2);
    addMinutelyTask(new SteamPlaySessionGenerator(connection, person_id),
        10);
    addMinutelyTask(new TVDBUpdateRunner(connection, tvdbjwtProvider, jsonReader, socket, UpdateMode.SANITY),
        4);
    addMinutelyTask(new IGDBUpdateRunner(connection, igdbProvider, jsonReader, UpdateMode.SMART),
        30);
    addMinutelyTask(new TVDBUpdateRunner(connection, tvdbjwtProvider, jsonReader, socket, UpdateMode.SMART),
        30);
    addMinutelyTask(new MetacriticTVUpdateRunner(connection, UpdateMode.CHUNKED),
        7);
    addMinutelyTask(new MetacriticTVUpdateRunner(connection, UpdateMode.SANITY),
        14);
    addMinutelyTask(new HowLongToBeatUpdateRunner(connection, UpdateMode.QUICK, howLongServiceHandler, chromeProvider),
        30);

    // HOURLY
    addHourlyTask(new SteamGameUpdateRunner(connection, person_id, steamProvider, chromeProvider, igdbProvider, jsonReader),
        1);
    addHourlyTask(new HowLongToBeatUpdateRunner(connection, UpdateMode.PING, howLongServiceHandler, chromeProvider),
        1);
    addHourlyTask(new IGDBUpdateRunner(connection, igdbProvider, jsonReader, UpdateMode.SANITY),
        24);
    addHourlyTask(new MetacriticGameUpdateRunner(connection, UpdateMode.SMART, person_id),
        24);
    addHourlyTask(new EpisodeGroupUpdater(connection, null),
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
    return !executionEnvironment.isLocal();
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

    private final TaskScheduleRunner runner;
    private final PeriodicTaskSchedule nextTask;

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
