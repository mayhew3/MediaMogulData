package com.mayhew3.mediamogul.games;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.mayhew3.mediamogul.EnvironmentChecker;
import com.mayhew3.mediamogul.exception.MissingEnvException;
import com.mayhew3.mediamogul.scheduler.TaskScheduleRunner;
import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
import com.mayhew3.mediamogul.games.provider.SteamProvider;
import com.mayhew3.mediamogul.games.provider.SteamProviderImpl;
import com.mayhew3.mediamogul.model.games.Game;
import com.mayhew3.mediamogul.model.games.GameLog;
import com.mayhew3.mediamogul.model.games.PersonGame;
import com.mayhew3.mediamogul.scheduler.UpdateRunner;
import com.mayhew3.mediamogul.tv.helper.UpdateMode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

public class SteamGameUpdater implements UpdateRunner {

  private SQLConnection connection;
  private Integer person_id;
  private SteamProvider steamProvider;

  private static Logger logger = LogManager.getLogger(SteamGameUpdater.class);

  public SteamGameUpdater(SQLConnection connection, Integer person_id, SteamProvider steamProvider) {
    this.connection = connection;
    this.person_id = person_id;
    this.steamProvider = steamProvider;
  }

  public static void main(String... args) throws SQLException, FileNotFoundException, URISyntaxException, MissingEnvException {
    List<String> argList = Lists.newArrayList(args);
    boolean logToFile = argList.contains("LogToFile");
    ArgumentChecker argumentChecker = new ArgumentChecker(args);

    String personIDString = EnvironmentChecker.getOrThrow("MediaMogulPersonID");
    assert personIDString != null;

    Integer person_id = Integer.parseInt(personIDString);

    if (logToFile) {
      SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
      String dateFormatted = simpleDateFormat.format(new Date());

      String mediaMogulLogs = System.getenv("MediaMogulLogs");

      File errorFile = new File(mediaMogulLogs + "\\SteamUpdaterErrors_" + dateFormatted + "_" + argumentChecker.getDBIdentifier() + ".log");
      FileOutputStream errorStream = new FileOutputStream(errorFile, true);
      PrintStream ps = new PrintStream(errorStream);
      System.setErr(ps);

      System.err.println("Starting run on " + new Date());

      File logFile = new File(mediaMogulLogs + "\\SteamUpdaterLog_" + dateFormatted + "_" + argumentChecker.getDBIdentifier() + ".log");
      FileOutputStream logStream = new FileOutputStream(logFile, true);
      PrintStream logPrintStream = new PrintStream(logStream);
      System.setOut(logPrintStream);
    }

    debug("");
    debug("SESSION START! Date: " + new Date());
    debug("");

    SQLConnection connection = PostgresConnectionFactory.createConnection(argumentChecker);
    SteamGameUpdater steamGameUpdater = new SteamGameUpdater(connection, person_id, new SteamProviderImpl());
    steamGameUpdater.runUpdate();

    debug(" --- ");
    debug(" Full operation complete!");
  }

  public void runUpdate() throws SQLException {
    Map<Integer, String> unfoundGames = new HashMap<>();
    ArrayList<String> duplicateGames = new ArrayList<>();

    Set<Integer> jsonSteamIDs = Sets.newHashSet();

    try {
      JSONObject jsonObject = steamProvider.getSteamInfo();
      JSONArray jsonArray = jsonObject.getJSONObject("response").getJSONArray("games");

      for (int i = 0; i < jsonArray.length(); i++) {
        JSONObject jsonGame = jsonArray.getJSONObject(i);

        processSteamGame(unfoundGames, duplicateGames, jsonGame);

        jsonSteamIDs.add(jsonGame.getInt("appid"));
      }

      debug("");
      debug("Updating ownership of games no longer in steam library...");
      debug("");

      ResultSet resultSet = connection.executeQuery("SELECT * FROM game WHERE steamid is not null AND owned = 'owned'");

      while (resultSet.next()) {
        Integer steamid = resultSet.getInt("steamid");

        if (!jsonSteamIDs.contains(steamid)) {
          debug(resultSet.getString("title") + ": no longer found!");

          Game game = new Game();
          game.initializeFromDBObject(resultSet);
          game.owned.changeValue("not owned");
          game.commit(connection);

          Optional<PersonGame> personGameOptional = game.getPersonGame(person_id, connection);
          if (personGameOptional.isPresent()) {
            PersonGame personGame = personGameOptional.get();
            personGame.retire();
            personGame.commit(connection);
          }
        }
      }

      debug("Operation finished!");
    } catch (IOException e) {
      debug("Error reading from URL: " + steamProvider.getFullUrl());
      e.printStackTrace();
    }

  }

  private void processSteamGame(Map<Integer, String> unfoundGames, ArrayList<String> duplicateGames, JSONObject jsonGame) throws SQLException {
    String name = jsonGame.getString("name");
    Integer steamID = jsonGame.getInt("appid");
    Integer playtime = jsonGame.getInt("playtime_forever");
    String icon = jsonGame.getString("img_icon_url");
    String logo = jsonGame.getString("img_logo_url");

    debug(name + ": looking for updates.");

    // if changed (match on appid, not name):
    // - update playtime
    // - update "Steam Name" column
    // - add log that it changed? How to know if it just differs from what I have,
    //    of if I've actually played since that was there? Confirmed flag? Or
    //    log is just a stamp of the date of checking and the total time?
    //    LOG: Previous Hours, Current Hours, Change. For first time, Prev and Curr are the same, and Change is 0.

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch("SELECT * FROM game WHERE steamid = ?", steamID);

    Game game = new Game();
    if (resultSet.next()) {
      game.initializeFromDBObject(resultSet);
      updateGame(name, steamID, playtime, icon, logo, game);
    } else {
      debug(" - Game not found! Adding.");
      addNewGame(name, steamID, playtime, icon, logo, game);
      unfoundGames.put(steamID, name);
    }

    if (resultSet.next()) {
      duplicateGames.add(name + "(" + steamID + ")");
    }
  }

  private void updateGame(String name, Integer steamID, Integer playtime, String icon, String logo, Game game) throws SQLException {
    game.logo.changeValue(logo);
    game.icon.changeValue(icon);
    game.steam_title.changeValue(name);
    game.owned.changeValue("owned");

    PersonGame personGame = game.getOrCreatePersonGame(person_id, connection);

    Integer previousPlaytime = personGame.minutes_played.getValue() == null ? 0 : personGame.minutes_played.getValue();
    if (!(playtime.compareTo(previousPlaytime) == 0)) {
      logUpdateToPlaytime(name, steamID, new BigDecimal(previousPlaytime), new BigDecimal(playtime), game.id.getValue());
      personGame.minutes_played.changeValue(playtime);
      personGame.last_played.changeValue(new Timestamp(bumpDateIfLateNight().toDate().getTime()));
    }

    personGame.commit(connection);
    game.commit(connection);
  }

  private void addNewGame(String name, Integer steamID, Integer playtime, String icon, String logo, Game game) throws SQLException {
    game.initializeForInsert();

    boolean needsPlaytimeUpdate = playtime > 0;

    game.platform.changeValue("Steam");
    game.owned.changeValue("owned");
    game.title.changeValue(name);
    game.steam_title.changeValue(name);
    game.steamID.changeValue(steamID);
    game.icon.changeValue(icon);
    game.logo.changeValue(logo);
    game.metacriticPage.changeValue(false);

    game.commit(connection);

    PersonGame personGame = new PersonGame();
    personGame.initializeForInsert();
    personGame.game_id.changeValue(game.id.getValue());
    personGame.person_id.changeValue(person_id);
    personGame.tier.changeValue(2);
    personGame.minutes_played.changeValue(playtime);

    personGame.commit(connection);

    if (needsPlaytimeUpdate) {
      logUpdateToPlaytime(name, steamID, BigDecimal.ZERO, new BigDecimal(playtime), game.id.getValue());
      personGame.last_played.changeValue(new Timestamp(bumpDateIfLateNight().toDate().getTime()));
      personGame.commit(connection);
    }
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

  protected static void debug(Object message) {
    logger.debug(message);
  }

  @Override
  public String getRunnerName() {
    return "Steam Game Updater";
  }

  @Override
  public @Nullable UpdateMode getUpdateMode() {
    return null;
  }
}
