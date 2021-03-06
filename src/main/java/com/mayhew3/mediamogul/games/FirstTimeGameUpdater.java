package com.mayhew3.mediamogul.games;

import com.mayhew3.mediamogul.ExternalServiceHandler;
import com.mayhew3.mediamogul.exception.SingleFailedException;
import com.mayhew3.mediamogul.games.exception.GameFailedException;
import com.mayhew3.mediamogul.games.provider.IGDBProvider;
import com.mayhew3.mediamogul.model.games.AvailableGamePlatform;
import com.mayhew3.mediamogul.model.games.Game;
import com.mayhew3.mediamogul.xml.JSONReader;
import com.mayhew3.postgresobject.db.SQLConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.openqa.selenium.chrome.ChromeDriver;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

class FirstTimeGameUpdater {
  private Game game;
  private SQLConnection connection;
  private IGDBProvider igdbProvider;
  private JSONReader jsonReader;
  private ChromeDriver chromeDriver;
  private ExternalServiceHandler howLongServiceHandler;
  private String giantbomb_api_key;
  private Integer person_id;

  private static Logger logger = LogManager.getLogger(FirstTimeGameUpdater.class);

  FirstTimeGameUpdater(Game game,
                       SQLConnection connection,
                       IGDBProvider igdbProvider,
                       JSONReader jsonReader,
                       ChromeDriver chromeDriver,
                       ExternalServiceHandler howLongServiceHandler,
                       String giantbomb_api_key,
                       Integer person_id) {
    this.game = game;
    this.connection = connection;
    this.igdbProvider = igdbProvider;
    this.jsonReader = jsonReader;
    this.chromeDriver = chromeDriver;
    this.howLongServiceHandler = howLongServiceHandler;
    this.giantbomb_api_key = giantbomb_api_key;
    this.person_id = person_id;
  }

  void updateGame() throws SQLException {
    updateIGDB();
    updateHowLongToBeat();
    updateGiantBomb();
    updateMetacritic();
  }

  private void updateIGDB() throws SQLException {
    logger.info("Updating IGDB...");
    IGDBUpdater igdbUpdater = new IGDBUpdater(game, connection, igdbProvider, jsonReader);
    igdbUpdater.updateGame();
  }

  private void updateHowLongToBeat() throws SQLException {
    logger.info("Updating HowLongToBeat...");
    HowLongToBeatUpdater howLongToBeatUpdater = new HowLongToBeatUpdater(game, connection, chromeDriver, howLongServiceHandler);
    try {
      howLongToBeatUpdater.runUpdater();
    } catch (GameFailedException e) {
      logger.warn("Game failed how long update: " + game.title.getValue());
      logger.warn(e.getMessage());
    }
  }

  private void updateMetacritic() throws SQLException {
    logger.info("Updating Metacritic...");
    List<AvailableGamePlatform> platforms = game.getAvailableGamePlatforms(connection);
    ArrayList<DateTime> filledDates = new ArrayList<>();
    for (AvailableGamePlatform platform : platforms) {
      MetacriticGameUpdater metacriticGameUpdater = new MetacriticGameUpdater(game, connection, person_id, platform, filledDates);
      try {
        metacriticGameUpdater.runUpdater();
        logger.info("Found metacritic for " + game.title.getValue() + " (" + platform.platformName.getValue() + "): " + platform.metacritic.getValue());
      } catch (SingleFailedException e) {
        logger.warn("Game failed metacritic update: " + game.title.getValue());
        logger.warn(e.getMessage());
      }
    }

  }

  private void updateGiantBomb() throws SQLException {
    logger.info("Updating GiantBomb...");
    GiantBombUpdater giantBombUpdater = new GiantBombUpdater(game, connection, giantbomb_api_key);
    try {
      giantBombUpdater.updateGame();
    } catch (InterruptedException e) {
      logger.warn("Game failed giantbomb update: " + game.title.getValue());
      e.printStackTrace();
    }
  }
}
