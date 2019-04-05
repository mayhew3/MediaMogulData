package com.mayhew3.mediamogul.games;

import com.mayhew3.mediamogul.ExternalServiceHandler;
import com.mayhew3.mediamogul.games.provider.IGDBProvider;
import com.mayhew3.mediamogul.model.games.Game;
import com.mayhew3.mediamogul.xml.JSONReader;
import com.mayhew3.postgresobject.db.SQLConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openqa.selenium.chrome.ChromeDriver;

import java.sql.SQLException;

public class FirstTimeGameUpdater {
  private Game game;
  private SQLConnection connection;
  private IGDBProvider igdbProvider;
  private JSONReader jsonReader;
  private ChromeDriver chromeDriver;
  private ExternalServiceHandler howLongServiceHandler;

  private static Logger logger = LogManager.getLogger(FirstTimeGameUpdater.class);

  public FirstTimeGameUpdater(Game game, SQLConnection connection, IGDBProvider igdbProvider, JSONReader jsonReader, ChromeDriver chromeDriver, ExternalServiceHandler howLongServiceHandler) {
    this.game = game;
    this.connection = connection;
    this.igdbProvider = igdbProvider;
    this.jsonReader = jsonReader;
    this.chromeDriver = chromeDriver;
    this.howLongServiceHandler = howLongServiceHandler;
  }

  public void updateGame() throws SQLException {
    updateIGDB();
    updateHowLongToBeat();
  }

  private void updateIGDB() throws SQLException {
    IGDBUpdater igdbUpdater = new IGDBUpdater(game, connection, igdbProvider, jsonReader);
    igdbUpdater.updateGame();
  }

  private void updateHowLongToBeat() throws SQLException {
    HowLongToBeatUpdater howLongToBeatUpdater = new HowLongToBeatUpdater(game, connection, chromeDriver, howLongServiceHandler);
    try {
      howLongToBeatUpdater.runUpdater();
    } catch (GameFailedException e) {
      logger.warn("Game failed how long update: " + game.title.getValue());
      e.printStackTrace();
    }
  }

}
