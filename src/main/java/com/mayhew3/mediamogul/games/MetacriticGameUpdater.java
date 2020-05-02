package com.mayhew3.mediamogul.games;

import com.google.common.collect.Maps;
import com.mayhew3.mediamogul.MetacriticUpdater;
import com.mayhew3.mediamogul.model.games.AvailableGamePlatform;
import com.mayhew3.mediamogul.model.games.Game;
import com.mayhew3.mediamogul.model.games.GameLog;
import com.mayhew3.mediamogul.model.games.GamePlatform;
import com.mayhew3.postgresobject.db.SQLConnection;
import org.jetbrains.annotations.Nullable;
import org.jsoup.nodes.Document;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;

public class MetacriticGameUpdater extends MetacriticUpdater {

  private final Game game;
  private final Integer person_id;
  private final AvailableGamePlatform availablePlatform;

  public MetacriticGameUpdater(Game game, SQLConnection connection, Integer person_id, AvailableGamePlatform availablePlatform) {
    super(connection);
    this.game = game;
    this.person_id = person_id;
    this.availablePlatform = availablePlatform;
  }

  public void runUpdater() throws SingleFailedException, SQLException {
    parseMetacritic();
  }

  private void parseMetacritic() throws SingleFailedException, SQLException {
    String title = game.title.getValue();
    String hint = game.metacriticHint.getValue();
    String platformName = availablePlatform.platformName.getValue();
    String formattedTitle = formatTitle(title, hint);
    GamePlatform gamePlatform = availablePlatform.getGamePlatform(connection);
    String formattedPlatform = formatPlatform(gamePlatform);

    if (formattedPlatform == null) {
      throw new GameFailedException("No platform mapping for platform '" + platformName + "'");
    } else {
      String prefix = "game/" + formattedPlatform + "/" + formattedTitle;

      Document document = getDocument(prefix, title);

      availablePlatform.metacriticPage.changeValue(true);
      game.commit(connection);

      int metaCritic = getMetacriticFromDocument(document);

      availablePlatform.metacriticMatched.changeValue(new Timestamp(new Date().getTime()));

      BigDecimal previousValue = availablePlatform.metacritic.getValue();
      BigDecimal updatedValue = new BigDecimal(metaCritic);

      availablePlatform.metacritic.changeValue(updatedValue);

      if (previousValue == null || previousValue.compareTo(updatedValue) != 0) {
        createGameLog(title, platformName, previousValue, updatedValue);
      }

      availablePlatform.commit(connection);
    }
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

  @Nullable
  private String formatPlatform(GamePlatform platform) {
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
    formattedPlatforms.put("Switch", "switch");

    String mapMatch = formattedPlatforms.get(platform.fullName.getValue());
    if (mapMatch == null) {
      String shortName = platform.shortName.getValue();
      if (shortName != null) {
        return shortName.replace(" ", "-");
      } else {
        return null;
      }
    } else {
      return mapMatch;
    }
  }

}
