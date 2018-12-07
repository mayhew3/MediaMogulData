package com.mayhew3.mediamogul.games;

import com.google.common.collect.Lists;
import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
import com.mayhew3.mediamogul.model.games.Game;
import com.mayhew3.mediamogul.scheduler.UpdateRunner;
import com.mayhew3.mediamogul.tv.helper.UpdateMode;
import org.jetbrains.annotations.Nullable;
import org.joda.time.DateTime;
import org.openqa.selenium.WebDriverException;
import org.openqa.selenium.chrome.ChromeDriver;

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

  private final Map<UpdateMode, Runnable> methodMap;

  public HowLongToBeatUpdateRunner(SQLConnection connection, UpdateMode updateMode) {
    methodMap = new HashMap<>();
    methodMap.put(UpdateMode.QUICK, this::runUpdateFull);
    methodMap.put(UpdateMode.FULL, this::runUpdateOnAllFailed);
    methodMap.put(UpdateMode.SINGLE, this::runUpdateOnSingle);

    this.connection = connection;

    if (!methodMap.keySet().contains(updateMode)) {
      throw new IllegalArgumentException("Update type '" + updateMode + "' is not applicable for this updater.");
    }

    this.updateMode = updateMode;
  }

  public static void main(String[] args) throws FileNotFoundException, SQLException, URISyntaxException {
    List<String> argList = Lists.newArrayList(args);
    Boolean logToFile = argList.contains("LogToFile");
    ArgumentChecker argumentChecker = new ArgumentChecker(args);

    if (logToFile) {
      String mediaMogulLogs = System.getenv("MediaMogulLogs");

      File file = new File(mediaMogulLogs + "\\HowLongToBeatUpdater.log");
      FileOutputStream fos = new FileOutputStream(file, true);
      PrintStream ps = new PrintStream(fos);
      System.setErr(ps);
      System.setOut(ps);
    }

    UpdateMode updateMode = UpdateMode.getUpdateModeOrDefault(argumentChecker, UpdateMode.QUICK);
    SQLConnection connection = PostgresConnectionFactory.createConnection(argumentChecker);

    setDriverPath();

    HowLongToBeatUpdateRunner updateRunner = new HowLongToBeatUpdateRunner(connection, updateMode);
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

  private void runUpdateFull() {
    Date date = new DateTime().minusDays(7).toDate();
    Timestamp timestamp = new Timestamp(date.getTime());

    String sql = "SELECT * FROM game WHERE howlong_updated IS NULL "
        + " AND (howlong_failed IS NULL OR howlong_failed < ?)";

    try {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, timestamp);

      runUpdateOnResultSet(resultSet);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void runUpdateOnAllFailed() {
    String sql = "SELECT * FROM game WHERE howlong_updated IS NULL ";

    try {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql);
      runUpdateOnResultSet(resultSet);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void runUpdateOnSingle() {
    String gameTitle = "Middle-earth™: Shadow of War™";
    String sql = "SELECT * FROM game WHERE title = ? ";

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

    ChromeDriver chromeDriver = new ChromeDriver();

    while (resultSet.next()) {
      Game game = new Game();
      try {
        game.initializeFromDBObject(resultSet);

        debug("Updating game: " + game);

        HowLongToBeatUpdater updater = new HowLongToBeatUpdater(game, connection, chromeDriver);
        updater.runUpdater();
      } catch (SQLException e) {
        e.printStackTrace();
        debug("Game failed to load from DB.");
        failures++;
      } catch (GameFailedException e) {
        e.printStackTrace();
        debug("Game failed: " + game);
        logFailure(game);
        failures++;
      } catch (WebDriverException e) {
        e.printStackTrace();
        debug("WebDriver error: " + game);
        logFailure(game);
        failures++;
      }

      debug(i + " processed.");
      i++;
    }

    debug("Operation completed! Failed on " + failures + "/" + (i-1) + " games (" + (failures/i*100) + "%)");

    chromeDriver.close();
  }

  private void logFailure(Game game) throws SQLException {
    game.howlong_failed.changeValue(new Timestamp(new Date().getTime()));
    game.commit(connection);
  }

  private static void setDriverPath() {
    String driverPath = System.getProperty("user.dir") + "\\resources\\chromedriver.exe";
    System.setProperty("webdriver.chrome.driver", driverPath);
  }

  protected void debug(Object object) {
    System.out.println(object);
  }
}

