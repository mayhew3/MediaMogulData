package com.mayhew3.mediamogul.games;

import com.google.common.collect.Maps;
import com.mayhew3.mediamogul.MetacriticUpdater;
import com.mayhew3.mediamogul.model.games.AvailableGamePlatform;
import com.mayhew3.mediamogul.model.games.Game;
import com.mayhew3.mediamogul.model.games.GameLog;
import com.mayhew3.postgresobject.db.SQLConnection;
import org.jsoup.nodes.Document;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;

public class MetacriticGameUpdater extends MetacriticUpdater {

  private final Game game;
  private final Integer person_id;
  private final AvailableGamePlatform platform;

  public MetacriticGameUpdater(Game game, SQLConnection connection, Integer person_id, AvailableGamePlatform platform) {
    super(connection);
    this.game = game;
    this.person_id = person_id;
    this.platform = platform;
  }

  public void runUpdater() throws SingleFailedException, SQLException {
    parseMetacritic();
  }

  private void parseMetacritic() throws SingleFailedException, SQLException {
    String title = game.title.getValue();
    String hint = game.metacriticHint.getValue();
    String platformName = platform.platformName.getValue();
    String formattedTitle = formatTitle(title, hint);
    String formattedPlatform = formatPlatform(platformName);

    String prefix = "game/" + formattedPlatform + "/" + formattedTitle;

    Document document = getDocument(prefix, title);

    platform.metacriticPage.changeValue(true);
    game.commit(connection);

    int metaCritic = getMetacriticFromDocument(document);

    platform.metacriticMatched.changeValue(new Timestamp(new Date().getTime()));

    BigDecimal previousValue = platform.metacritic.getValue();
    BigDecimal updatedValue = new BigDecimal(metaCritic);

    platform.metacritic.changeValue(updatedValue);

    if (previousValue == null || previousValue.compareTo(updatedValue) != 0) {
      createGameLog(title, platformName, previousValue, updatedValue);
    }

    platform.commit(connection);
  }

  private void createGameLog(String title, String platform, BigDecimal previousValue, BigDecimal updatedValue) throws SQLException {
    GameLog gameLog = new GameLog();
    gameLog.initializeForInsert();

    gameLog.game.changeValue(title);
    gameLog.steamID.changeValue(game.steamID.getValue());
    gameLog.platform.changeValue(platform);
    gameLog.previousPlaytime.changeValue(previousValue);
    gameLog.updatedplaytime.changeValue(updatedValue);

    if (previousValue != null) {
      gameLog.diff.changeValue(updatedValue.subtract(previousValue));
    }

    gameLog.eventtype.changeValue("Metacritic");
    gameLog.eventdate.changeValue(new Timestamp(new Date().getTime()));

    gameLog.person_id.changeValue(person_id);
    gameLog.gameID.changeValue(game.id.getValue());
    gameLog.commit(connection);
  }

  private String formatPlatform(String platform) {
    Map<String, String> formattedPlatforms = Maps.newHashMap();
    formattedPlatforms.put("PC", "pc");
    formattedPlatforms.put("Steam", "pc");
    formattedPlatforms.put("Xbox 360", "xbox-360");
    formattedPlatforms.put("Xbox One", "xbox-one");
    formattedPlatforms.put("PS3", "playstation-3");
    formattedPlatforms.put("PS4", "playstation-4");
    formattedPlatforms.put("Wii", "wii");
    formattedPlatforms.put("Wii U", "wii-u");
    formattedPlatforms.put("DS", "ds");
    formattedPlatforms.put("Xbox", "xbox");

    return formattedPlatforms.get(platform);
  }

}
