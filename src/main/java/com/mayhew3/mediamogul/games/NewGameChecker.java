package com.mayhew3.mediamogul.games;

import com.mashape.unirest.http.exceptions.UnirestException;
import com.mayhew3.mediamogul.ChromeProvider;
import com.mayhew3.mediamogul.EnvironmentChecker;
import com.mayhew3.mediamogul.ExternalServiceHandler;
import com.mayhew3.mediamogul.ExternalServiceType;
import com.mayhew3.mediamogul.games.provider.IGDBProvider;
import com.mayhew3.mediamogul.games.provider.IGDBProviderImpl;
import com.mayhew3.mediamogul.model.games.Game;
import com.mayhew3.mediamogul.scheduler.UpdateRunner;
import com.mayhew3.mediamogul.tv.helper.UpdateMode;
import com.mayhew3.mediamogul.xml.JSONReader;
import com.mayhew3.mediamogul.xml.JSONReaderImpl;
import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
import com.mayhew3.postgresobject.exception.MissingEnvException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import org.openqa.selenium.chrome.ChromeDriver;

import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;

public class NewGameChecker implements UpdateRunner {

  private SQLConnection connection;
  private JSONReader jsonReader;
  private IGDBProvider igdbProvider;
  private ChromeProvider chromeProvider;
  private ExternalServiceHandler howLongServiceHandler;
  private String giantbomb_api_key;
  private Integer person_id;

  private static Logger logger = LogManager.getLogger(NewGameChecker.class);

  public NewGameChecker(SQLConnection connection,
                        JSONReader jsonReader,
                        IGDBProvider igdbProvider,
                        ChromeProvider chromeProvider,
                        ExternalServiceHandler howLongServiceHandler,
                        Integer person_id) throws MissingEnvException {
    this.connection = connection;
    this.jsonReader = jsonReader;
    this.igdbProvider = igdbProvider;
    this.chromeProvider = chromeProvider;
    this.howLongServiceHandler = howLongServiceHandler;
    this.person_id = person_id;
    this.giantbomb_api_key = EnvironmentChecker.getOrThrow("giantbomb_api");
  }

  public static void main(String... args) throws URISyntaxException, SQLException, MissingEnvException, UnirestException {
    ArgumentChecker argumentChecker = new ArgumentChecker(args);
    SQLConnection connection = PostgresConnectionFactory.createConnection(argumentChecker);
    JSONReaderImpl jsonReader = new JSONReaderImpl();
    IGDBProviderImpl igdbProvider = new IGDBProviderImpl();
    ChromeProvider chromeProvider = new ChromeProvider();
    ExternalServiceHandler howLongToBeat = new ExternalServiceHandler(connection, ExternalServiceType.HowLongToBeat);
    String mediaMogulPersonID = EnvironmentChecker.getOrThrow("MediaMogulPersonID");
    Integer person_id = Integer.parseInt(mediaMogulPersonID);

    NewGameChecker newGameChecker = new NewGameChecker(connection, jsonReader, igdbProvider, chromeProvider, howLongToBeat, person_id);
    newGameChecker.runUpdate();
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
        "FROM valid_game " +
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

      try {
        // DO STUFF
        logger.info("New Game found: '" + game.title.getValue() + "'");
        FirstTimeGameUpdater firstTimeGameUpdater = new FirstTimeGameUpdater(
            game,
            connection,
            igdbProvider,
            jsonReader,
            chromeDriver,
            howLongServiceHandler,
            giantbomb_api_key,
            person_id);
        firstTimeGameUpdater.updateGame();
        logger.info("Finished processing game '" + game.title.getValue() + "'");

      } catch (Exception e) {
        logger.error("Uncaught exception during processing of game: '" + game.title.getValue() + "'");
        e.printStackTrace();
      } finally {
        game.first_processed.changeValue(true);
        game.commit(connection);
      }

    }

    if (browserOpen) {
      chromeProvider.closeBrowser();
    }
  }

  private Boolean hasRows(ResultSet resultSet) throws SQLException {
    return resultSet.isBeforeFirst();
  }

}
