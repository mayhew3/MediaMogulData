package com.mayhew3.mediamogul.games;

import com.google.common.collect.Lists;
import com.mayhew3.mediamogul.ChromeProvider;
import com.mayhew3.mediamogul.ExternalServiceHandler;
import com.mayhew3.mediamogul.ExternalServiceType;
import com.mayhew3.mediamogul.games.exception.GameFailedException;
import com.mayhew3.mediamogul.model.games.Game;
import com.mayhew3.mediamogul.scheduler.UpdateRunner;
import com.mayhew3.mediamogul.tv.helper.UpdateMode;
import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.EnvironmentChecker;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
import com.mayhew3.postgresobject.exception.MissingEnvException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebDriverException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HowLongToBeatUpdateRunner implements UpdateRunner {

  private SQLConnection connection;
  private UpdateMode updateMode;
  private ExternalServiceHandler howLongServiceHandler;
  private ChromeProvider chromeProvider;

  private final Map<UpdateMode, Runnable> methodMap;

  private static Logger logger = LogManager.getLogger(HowLongToBeatUpdateRunner.class);

  public HowLongToBeatUpdateRunner(SQLConnection connection, UpdateMode updateMode, ExternalServiceHandler howLongServiceHandler, ChromeProvider chromeProvider) {
    this.chromeProvider = chromeProvider;
    methodMap = new HashMap<>();
    methodMap.put(UpdateMode.QUICK, this::runUpdateQuick);
    methodMap.put(UpdateMode.FULL, this::runUpdateFull);
    methodMap.put(UpdateMode.SINGLE, this::runUpdateOnSingle);
    methodMap.put(UpdateMode.PING, this::runUpdateOnSingleForPing);

    this.connection = connection;

    if (!methodMap.keySet().contains(updateMode)) {
      throw new IllegalArgumentException("Update type '" + updateMode + "' is not applicable for this updater.");
    }

    this.updateMode = updateMode;
    this.howLongServiceHandler = howLongServiceHandler;
  }

  public static void main(String[] args) throws FileNotFoundException, SQLException, URISyntaxException, MissingEnvException {
    List<String> argList = Lists.newArrayList(args);
    Boolean logToFile = argList.contains("LogToFile");
    ArgumentChecker argumentChecker = new ArgumentChecker(args);

    if (logToFile) {
      String mediaMogulLogs = EnvironmentChecker.getOrThrow("MediaMogulLogs");

      File file = new File(mediaMogulLogs + "\\HowLongToBeatUpdater.log");
      FileOutputStream fos = new FileOutputStream(file, true);
      PrintStream ps = new PrintStream(fos);
      System.setErr(ps);
      System.setOut(ps);
    }

    UpdateMode updateMode = UpdateMode.getUpdateModeOrDefault(argumentChecker, UpdateMode.QUICK);
    SQLConnection connection = PostgresConnectionFactory.createConnection(argumentChecker);
    ExternalServiceHandler howLongServiceHandler = new ExternalServiceHandler(connection, ExternalServiceType.HowLongToBeat);

    HowLongToBeatUpdateRunner updateRunner = new HowLongToBeatUpdateRunner(connection, updateMode, howLongServiceHandler, new ChromeProvider());
    updateRunner.runUpdate();
  }

  @Override
  public String getRunnerName() {
    return "HowLongToBeat Updater";
  }

  @Override
  public @Nullable UpdateMode getUpdateMode() {
    return updateMode;
  }

  @Override
  public void runUpdate() {
    methodMap.get(updateMode).run();
  }

  private void runUpdateQuick() {

    String sql = "SELECT * " +
        "FROM valid_game " +
        "WHERE howlong_updated IS NULL " +
        "ORDER BY howlong_failed NULLS FIRST, id " +
        "LIMIT 1 ";

    try {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql);

      runUpdateOnResultSet(resultSet);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void runUpdateFull() {
    String sql = "SELECT * " +
        "FROM valid_game " +
        "WHERE howlong_updated IS NULL";

    try {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql);
      runUpdateOnResultSet(resultSet);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void runUpdateOnSingle() {
    String gameTitle = "Return of the Obra Dinn";
    String sql = "SELECT * " +
        "FROM valid_game " +
        "WHERE title = ? ";

    try {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, gameTitle);
      runUpdateOnResultSet(resultSet);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  /*
  * Special Single-mode update on a game we know works to verify updater is still working correctly.
  * */
  private void runUpdateOnSingleForPing() {
    String gameTitle = "Return of the Obra Dinn";
    String sql = "SELECT * " +
        "FROM valid_game " +
        "WHERE title = ? ";

    try {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, gameTitle);
      runUpdateOnResultSet(resultSet);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void runUpdateOnResultSet(ResultSet resultSet) throws SQLException {
    int i = 1;
    int failures = 0;

    WebDriver chromeDriver = chromeProvider.openBrowser();

    while (resultSet.next()) {
      Game game = new Game();
      try {
        game.initializeFromDBObject(resultSet);

        debug("Updating game: " + game);

        HowLongToBeatUpdater updater = new HowLongToBeatUpdater(game, connection, chromeDriver, howLongServiceHandler);
        updater.runUpdater();

      } catch (SQLException e) {
        e.printStackTrace();
        logger.error("Game failed to load from DB.");
        howLongServiceHandler.connectionFailed();
        failures++;
      } catch (GameFailedException e) {
        logger.warn(e.getMessage());
        logger.warn("Game failed: " + game);
        howLongServiceHandler.connectionFailed();
        logFailure(game);
        failures++;
      } catch (WebDriverException e) {
        e.printStackTrace();
        logger.error("WebDriver error: " + game);
        howLongServiceHandler.connectionFailed();
        logFailure(game);
        failures++;
      }

      debug(i + " processed.");
      i++;
    }

    logger.info("Operation completed! Failed on " + failures + "/" + (i-1) + " games (" + (failures/i*100) + "%)");

    chromeProvider.closeBrowser();
  }

  private void logFailure(Game game) throws SQLException {
    game.howlong_failed.changeValue(new Timestamp(new Date().getTime()));
    game.commit(connection);
  }

  private static void setDriverPath() {
    String driverPath = System.getProperty("user.dir") + "\\resources\\chromedriver.exe";
    System.setProperty("webdriver.chrome.driver", driverPath);
  }

  private void debug(Object message) {
    logger.debug(message);
  }
}

