package com.mayhew3.mediamogul.games;

import com.google.common.collect.Maps;
import com.mayhew3.mediamogul.MetacriticUpdater;
import com.mayhew3.mediamogul.games.exception.MetacriticElementNotFoundException;
import com.mayhew3.mediamogul.games.exception.MetacriticPageNotFoundException;
import com.mayhew3.mediamogul.games.exception.MetacriticPlatformNameException;
import com.mayhew3.mediamogul.model.games.AvailableGamePlatform;
import com.mayhew3.mediamogul.model.games.Game;
import com.mayhew3.mediamogul.model.games.GameLog;
import com.mayhew3.mediamogul.model.games.GamePlatform;
import com.mayhew3.postgresobject.db.SQLConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.jsoup.nodes.Document;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class MetacriticGameUpdater extends MetacriticUpdater {

  private final Game game;
  private final Integer person_id;
  private final AvailableGamePlatform availablePlatform;

  private final List<DateTime> filledDates;

  @SuppressWarnings("FieldCanBeLocal")
  private final int MAX_GAMES_PER_DAY = 60;
  @SuppressWarnings("FieldCanBeLocal")
  private final int SANITY_THRESHOLD = 90;

  private static final Logger logger = LogManager.getLogger(FirstTimeGameUpdater.class);

  public MetacriticGameUpdater(Game game, SQLConnection connection, Integer person_id, AvailableGamePlatform availablePlatform, List<DateTime> filledDates) {
    super(connection);
    this.game = game;
    this.person_id = person_id;
    this.availablePlatform = availablePlatform;
    this.filledDates = filledDates;
  }

  public void runUpdater() throws MetacriticElementNotFoundException, MetacriticPageNotFoundException, MetacriticPlatformNameException, SQLException {
    try {
      parseMetacritic();
    } catch (MetacriticPageNotFoundException | MetacriticElementNotFoundException | MetacriticPlatformNameException e) {
      this.availablePlatform.metacritic_failed.changeValue(new Date());
      throw e;
    } finally {
      setNextUpdate();
    }
  }

  private void parseMetacritic() throws MetacriticPageNotFoundException, MetacriticElementNotFoundException, SQLException, MetacriticPlatformNameException {
    String title = game.title.getValue();
    String hint = game.metacriticHint.getValue();
    String platformName = availablePlatform.platformName.getValue();
    String formattedTitle = formatTitle(title, hint);
    GamePlatform gamePlatform = availablePlatform.getGamePlatform(connection);
    String formattedPlatform = formatPlatform(gamePlatform);

    if (formattedPlatform == null) {
      throw new MetacriticPlatformNameException("No platform mapping for platform '" + platformName + "'");
    } else {
      String prefix = "game/" + formattedPlatform + "/" + formattedTitle;

      Document document = getGameDocument(title, prefix);

      availablePlatform.metacritic_page.changeValue(true);
      game.commit(connection);

      int metaCritic = getGameMetacriticFromDocument(document);

      availablePlatform.metacritic_matched.changeValue(new Date());

      BigDecimal previousValue = availablePlatform.metacritic.getValue();
      BigDecimal updatedValue = new BigDecimal(metaCritic);

      availablePlatform.metacritic.changeValue(updatedValue);

      if (previousValue == null || previousValue.compareTo(updatedValue) != 0) {
        createGameLog(title, platformName, previousValue, updatedValue);
      }

      availablePlatform.commit(connection);

      setNextUpdate();
    }
  }

  private Document getGameDocument(String title, String prefix) throws MetacriticPageNotFoundException {
    try {
      return getDocument(prefix, title);
    } catch (Exception e) {
      setNextUpdate();
      throw new MetacriticPageNotFoundException(e.getMessage());
    }
  }

  private Integer getGameMetacriticFromDocument(Document document) throws MetacriticElementNotFoundException {
    try {
      return getMetacriticFromDocument(document);
    } catch (Exception e) {
      setNextUpdate();
      throw new MetacriticElementNotFoundException(e.getMessage());
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
    if (platform.metacritic_uri.getValue() != null) {
      return platform.metacritic_uri.getValue();
    }

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
        return shortName.replace(" ", "-").toLowerCase();
      } else {
        return null;
      }
    } else {
      return mapMatch;
    }
  }

  private void setNextUpdate() {
    try {
      Timestamp firstReleaseDate = game.igdb_release_date.getValue();
      Integer days = calculateDaysBasedOnReleaseDate(firstReleaseDate);
      setNextUpdateWithMinimumDays(days);
    } catch (SQLException e) {
      logger.error("Unexpected SQL error updating metacritic_next_update!");
      throw new RuntimeException(e.getLocalizedMessage());
    }
  }

  private Integer calculateDaysBasedOnReleaseDate(Timestamp releaseDateTimestamp) {
    int minimumDays = 3;
    int maximumDays = 90;
    int maximumReleaseDays = 1000;
    if (releaseDateTimestamp == null) {
      return maximumDays;
    }
    DateTime releaseDate = new DateTime(releaseDateTimestamp);
    Days daysBetween = Days.daysBetween(releaseDate, new DateTime());
    int adjusted = Math.min(daysBetween.getDays(), maximumReleaseDays);
    adjusted = Math.max(adjusted, 0);
    double releaseRatio = (float)adjusted / (float)maximumReleaseDays;
    int daysBetweenMinAndMax = maximumDays - minimumDays;
    int daysAfterMinimum = (int)(releaseRatio * daysBetweenMinAndMax);
    return minimumDays + daysAfterMinimum;
  }

  private DateTime getNextDateThatIsntKnownFilled(DateTime startingDate) {
    DateTime currentDate = startingDate;
    while (filledDates.contains(currentDate.withTimeAtStartOfDay())) {
      currentDate = currentDate.plusDays(1);
    }
    return currentDate;
  }

  private void setNextUpdateWithMinimumDays(Integer minimumDays) throws SQLException {
    DateTime initialDate = new DateTime().plusDays(minimumDays);

    int i = 0;
    int countOfGamesQueued;
    DateTime currentDate = getNextDateThatIsntKnownFilled(initialDate).minusDays(1);
    do {
      currentDate = currentDate.plusDays(1);
      countOfGamesQueued = getCountOfGamesQueuedForDay(currentDate);

      if (countOfGamesQueued >= MAX_GAMES_PER_DAY) {
        filledDates.add(currentDate.withTimeAtStartOfDay());
      }

      i++;
    } while (countOfGamesQueued >= MAX_GAMES_PER_DAY && i < SANITY_THRESHOLD);

    if (countOfGamesQueued < MAX_GAMES_PER_DAY) {
      availablePlatform.metacritic_next_update.changeValue(currentDate.toDate());
      availablePlatform.commit(connection);
    } else {
      throw new IllegalStateException("Couldn't find a date with few enough queued games before hitting sanity threshold.");
    }
  }

  private Timestamp toTimestamp(DateTime dateTime) {
    return new Timestamp(dateTime.toDate().getTime());
  }

  private Integer getCountOfGamesQueuedForDay(DateTime dateTime) throws SQLException {
    DateTime startMidnight = dateTime.withTimeAtStartOfDay();
    DateTime endMidnight = startMidnight.plusDays(1);

    String sql = "SELECT COUNT(1) AS dayCount " +
        "FROM available_game_platform " +
        "WHERE metacritic_next_update BETWEEN ? AND ? ";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, toTimestamp(startMidnight), toTimestamp(endMidnight));
    if (resultSet.next()) {
      return resultSet.getInt("dayCount");
    } else {
      throw new IllegalStateException("Error fetching dayCount from database.");
    }
  }


}
