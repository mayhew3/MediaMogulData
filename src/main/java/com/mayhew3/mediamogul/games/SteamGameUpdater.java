package com.mayhew3.mediamogul.games;

import com.mayhew3.mediamogul.ChromeProvider;
import com.mayhew3.mediamogul.model.games.*;
import com.mayhew3.postgresobject.db.SQLConnection;
import org.joda.time.DateTime;
import org.openqa.selenium.chrome.ChromeDriver;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

class SteamGameUpdater {
  private final Game game;
  private final SQLConnection connection;
  private final ChromeProvider chromeProvider;
  private final Integer person_id;

  SteamGameUpdater(Game game, SQLConnection connection, ChromeProvider chromeProvider, Integer person_id) {
    this.game = game;
    this.connection = connection;
    this.chromeProvider = chromeProvider;
    this.person_id = person_id;
  }

  void updateGame(String name, Integer steamID, Integer playtime, String icon, String logo) throws SQLException {
    game.logo.changeValue(logo);
    game.icon.changeValue(icon);
    game.steam_title.changeValue(name);

    GamePlatform steamPlatform = GamePlatform.getOrCreatePlatform(connection, "Steam");

    AvailableGamePlatform availableGamePlatform = game.getOrCreatePlatform(steamPlatform, connection);

    PersonGame personGame = game.getOrCreatePersonGame(person_id, connection);
    MyGamePlatform myPlatform = personGame.getOrCreatePlatform(connection, availableGamePlatform);

    Integer previousPlaytime = myPlatform.minutes_played.getValue() == null ? 0 : myPlatform.minutes_played.getValue();
    if (!(playtime.compareTo(previousPlaytime) == 0)) {
      logUpdateToPlaytime(name, steamID, new BigDecimal(previousPlaytime), new BigDecimal(playtime), game.id.getValue());
      personGame.minutes_played.changeValue(playtime);
      personGame.last_played.changeValue(new Timestamp(bumpDateIfLateNight().toDate().getTime()));

      myPlatform.minutes_played.changeValue(playtime);
      myPlatform.last_played.changeValue(new Timestamp(bumpDateIfLateNight().toDate().getTime()));
    }

    myPlatform.commit(connection);
    personGame.commit(connection);

    personGame.getOrCreatePlatform(connection, availableGamePlatform);

    game.commit(connection);
  }

  void addNewGame(String name, Integer steamID, Integer playtime, String icon, String logo) throws SQLException, GameFailedException {
    game.initializeForInsert();

    boolean needsPlaytimeUpdate = playtime > 0;

    game.title.changeValue(name);
    game.steam_title.changeValue(name);
    game.steamID.changeValue(steamID);
    game.icon.changeValue(icon);
    game.logo.changeValue(logo);
    game.metacriticPage.changeValue(false);

    game.commit(connection);

    GamePlatform steamPlatform = GamePlatform.getOrCreatePlatform(connection, "Steam");

    AvailableGamePlatform availableGamePlatform = game.getOrCreatePlatform(steamPlatform, connection);
    availableGamePlatform.metacriticPage.changeValue(false);

    PersonGame personGame = new PersonGame();
    personGame.initializeForInsert();
    personGame.game_id.changeValue(game.id.getValue());
    personGame.person_id.changeValue(person_id);
    personGame.tier.changeValue(2);
    personGame.minutes_played.changeValue(playtime);

    MyGamePlatform myPlatform = personGame.getOrCreatePlatform(connection, availableGamePlatform);
    myPlatform.tier.changeValue(2);
    myPlatform.minutes_played.changeValue(playtime);

    if (needsPlaytimeUpdate) {
      logUpdateToPlaytime(name, steamID, BigDecimal.ZERO, new BigDecimal(playtime), game.id.getValue());
      personGame.last_played.changeValue(new Timestamp(bumpDateIfLateNight().toDate().getTime()));
      myPlatform.last_played.changeValue(new Timestamp(bumpDateIfLateNight().toDate().getTime()));
    }

    personGame.commit(connection);
    myPlatform.commit(connection);

    ChromeDriver chromeDriver = chromeProvider.openBrowser();

    SteamAttributeUpdater steamAttributeUpdater = new SteamAttributeUpdater(game, connection, chromeDriver);
    steamAttributeUpdater.runUpdater();

    chromeProvider.closeBrowser();

  }

  private void logUpdateToPlaytime(String name, Integer steamID, BigDecimal previousPlaytime, BigDecimal updatedPlaytime, Integer gameID) throws SQLException {
    GameLog gameLog = new GameLog();
    gameLog.initializeForInsert();

    gameLog.game.changeValue(name);
    gameLog.steamID.changeValue(steamID);
    gameLog.platform.changeValue("Steam");
    gameLog.previousPlaytime.changeValue(previousPlaytime);
    gameLog.updatedplaytime.changeValue(updatedPlaytime);
    gameLog.diff.changeValue(updatedPlaytime.subtract(previousPlaytime));
    gameLog.eventtype.changeValue("Played");
    gameLog.eventdate.changeValue(new Timestamp(new Date().getTime()));
    gameLog.gameID.changeValue(gameID);
    gameLog.person_id.changeValue(person_id);

    gameLog.commit(connection);
  }


  /**
   * Most of the time this updater runs at 4:45am, so if this method is called between 12am and 7am, and new playtime is
   * found, assume for now that it was played the previous day. Arbitrarily put 8pm.
   * @return The current timestamp if it is after 7am, or 8pm the previous day otherwise.
   */
  private static DateTime bumpDateIfLateNight() {
    DateTime today = new DateTime(new Date());
    if (today.getHourOfDay() > 7) {
      return today;
    } else {
      return today.minusDays(1).withHourOfDay(20);
    }
  }

}
