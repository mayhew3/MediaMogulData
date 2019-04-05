package com.mayhew3.mediamogul.games;

import com.mayhew3.mediamogul.ChromeProvider;
import com.mayhew3.mediamogul.EnvironmentChecker;
import com.mayhew3.mediamogul.ExternalServiceHandler;
import com.mayhew3.mediamogul.exception.MissingEnvException;
import com.mayhew3.mediamogul.games.provider.IGDBProvider;
import com.mayhew3.mediamogul.model.games.Game;
import com.mayhew3.mediamogul.scheduler.UpdateRunner;
import com.mayhew3.mediamogul.tv.helper.UpdateMode;
import com.mayhew3.mediamogul.xml.JSONReader;
import com.mayhew3.postgresobject.db.SQLConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import org.openqa.selenium.chrome.ChromeDriver;

import java.sql.ResultSet;
import java.sql.SQLException;

public class NewGameChecker implements UpdateRunner {

  private SQLConnection connection;
  private JSONReader jsonReader;
  private IGDBProvider igdbProvider;
  private ChromeProvider chromeProvider;
  private ExternalServiceHandler howLongServiceHandler;
  private String giantbomb_api_key;

  private static Logger logger = LogManager.getLogger(NewGameChecker.class);

  public NewGameChecker(SQLConnection connection, JSONReader jsonReader, IGDBProvider igdbProvider, ChromeProvider chromeProvider, ExternalServiceHandler howLongServiceHandler) throws MissingEnvException {
    this.connection = connection;
    this.jsonReader = jsonReader;
    this.igdbProvider = igdbProvider;
    this.chromeProvider = chromeProvider;
    this.howLongServiceHandler = howLongServiceHandler;
    this.giantbomb_api_key = EnvironmentChecker.getOrThrow("giantbomb_api");
  }

  @Override
  public String getRunnerName() {
    return "New Game Checker";
  }

  @Override
  public @Nullable UpdateMode getUpdateMode() {
    return null;
  }

  @Override
  public void runUpdate() throws SQLException {

    String sql = "SELECT * " +
        "FROM game " +
        "WHERE first_processed = ? ";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, false);

    ChromeDriver chromeDriver = null;
    boolean browserOpen = false;
    if (hasRows(resultSet)) {
      chromeDriver = chromeProvider.openBrowser();
      browserOpen = true;
    }

    while (resultSet.next()) {
      Game game = new Game();
      game.initializeFromDBObject(resultSet);

      // DO STUFF
      logger.info("New Game found: '" + game.title.getValue() + "'");
      FirstTimeGameUpdater firstTimeGameUpdater = new FirstTimeGameUpdater(
          game,
          connection,
          igdbProvider,
          jsonReader,
          chromeDriver,
          howLongServiceHandler,
          giantbomb_api_key);
      firstTimeGameUpdater.updateGame();
      logger.info("Finished processing game '" + game.title.getValue() + "'");

      game.first_processed.changeValue(true);
      game.commit(connection);
    }

    if (browserOpen) {
      chromeProvider.closeBrowser();
    }
  }

  private Boolean hasRows(ResultSet resultSet) throws SQLException {
    return resultSet.isBeforeFirst();
  }

}
