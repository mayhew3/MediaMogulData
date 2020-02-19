package com.mayhew3.mediamogul.games;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.mayhew3.mediamogul.ChromeProvider;
import com.mayhew3.mediamogul.EnvironmentChecker;
import com.mayhew3.mediamogul.exception.MissingEnvException;
import com.mayhew3.mediamogul.games.provider.SteamProvider;
import com.mayhew3.mediamogul.games.provider.SteamProviderImpl;
import com.mayhew3.mediamogul.model.games.Game;
import com.mayhew3.mediamogul.model.games.PersonGame;
import com.mayhew3.mediamogul.scheduler.UpdateRunner;
import com.mayhew3.mediamogul.tv.helper.UpdateMode;
import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

public class SteamGameUpdateRunner implements UpdateRunner {

  private SQLConnection connection;
  private Integer person_id;
  private SteamProvider steamProvider;
  private ChromeProvider chromeProvider;

  private static Logger logger = LogManager.getLogger(SteamGameUpdateRunner.class);

  public SteamGameUpdateRunner(SQLConnection connection, Integer person_id, SteamProvider steamProvider, ChromeProvider chromeProvider) {
    this.connection = connection;
    this.person_id = person_id;
    this.steamProvider = steamProvider;
    this.chromeProvider = chromeProvider;
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

    logger.debug("");
    logger.info("SESSION START! Date: " + new Date());
    logger.debug("");

    SQLConnection connection = PostgresConnectionFactory.createConnection(argumentChecker);
    SteamGameUpdateRunner steamGameUpdateRunner = new SteamGameUpdateRunner(connection, person_id, new SteamProviderImpl(), new ChromeProvider());
    steamGameUpdateRunner.runUpdate();

    logger.debug(" --- ");
    logger.info(" Full operation complete!");
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

        try {
          processSteamGame(unfoundGames, duplicateGames, jsonGame);
        } catch (GameFailedException e) {
          logger.error("Game failed: " + jsonGame);
          logger.warn(e.getMessage());
        }

        jsonSteamIDs.add(jsonGame.getInt("appid"));
      }

      logger.info("");
      logger.info("Updating ownership of games no longer in steam library...");
      logger.info("");

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

      logger.info("Operation finished!");
    } catch (IOException e) {
      logger.error("Error reading from URL: " + steamProvider.getFullUrl());
      e.printStackTrace();
    }

  }

  private void processSteamGame(Map<Integer, String> unfoundGames, ArrayList<String> duplicateGames, JSONObject jsonGame) throws SQLException, GameFailedException {
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
    SteamGameUpdater steamGameUpdater = new SteamGameUpdater(game, connection, chromeProvider, person_id);

    if (resultSet.next()) {
      game.initializeFromDBObject(resultSet);
      steamGameUpdater.updateGame(name, steamID, playtime, icon, logo);
    } else {
      debug(" - Game not found! Adding.");
      steamGameUpdater.addNewGame(name, steamID, playtime, icon, logo);
      unfoundGames.put(steamID, name);
    }

    if (resultSet.next()) {
      duplicateGames.add(name + "(" + steamID + ")");
    }
  }

  private static void debug(Object message) {
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
