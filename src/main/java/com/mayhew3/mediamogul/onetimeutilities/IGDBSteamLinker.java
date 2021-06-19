package com.mayhew3.mediamogul.onetimeutilities;

import com.mashape.unirest.http.exceptions.UnirestException;
import com.mayhew3.mediamogul.db.DatabaseEnvironments;
import com.mayhew3.mediamogul.games.provider.IGDBProvider;
import com.mayhew3.mediamogul.games.provider.IGDBProviderImpl;
import com.mayhew3.mediamogul.model.games.Game;
import com.mayhew3.mediamogul.xml.JSONReader;
import com.mayhew3.mediamogul.xml.JSONReaderImpl;
import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.db.DatabaseEnvironment;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
import com.mayhew3.postgresobject.exception.MissingEnvException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;

public class IGDBSteamLinker {

  private final SQLConnection connection;
  private final IGDBProvider igdbProvider;
  private final JSONReader jsonReader;

  private static final Logger logger = LogManager.getLogger(IGDBSteamLinker.class);

  public IGDBSteamLinker(SQLConnection connection, IGDBProvider igdbProvider, JSONReader jsonReader) {
    this.connection = connection;
    this.igdbProvider = igdbProvider;
    this.jsonReader = jsonReader;
  }

  public static void main(String... args) throws SQLException, MissingEnvException, URISyntaxException, UnirestException, InterruptedException {
    ArgumentChecker argumentChecker = new ArgumentChecker(args);

    DatabaseEnvironment environment = DatabaseEnvironments.getEnvironmentForDBArgument(argumentChecker);
    SQLConnection connection = PostgresConnectionFactory.createConnection(environment);

    IGDBSteamLinker igdbSteamLinker = new IGDBSteamLinker(connection, new IGDBProviderImpl(), new JSONReaderImpl());
    igdbSteamLinker.runUpdate();
  }

  private void runUpdate() throws SQLException, InterruptedException {
    String sql = "SELECT * " +
        "FROM game " +
        "WHERE steamid IS NULL " +
        "AND igdb_id IS NOT NULL ";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql);
    while (resultSet.next()) {
      Game game = new Game();
      game.initializeFromDBObject(resultSet);

      Integer igdbID = game.igdb_id.getValue();

      JSONArray updatedInfoArray = igdbProvider.getUpdatedInfo(igdbID);
      if (updatedInfoArray.length() != 1) {
        logger.debug("Expected exactly one match for game with igdb_id: " + game.igdb_id.getValue() + ", " +
            "but there are " + updatedInfoArray.length());
      } else {
        logger.debug(" - Found IGDB data matching existing ID. Updating.");
        JSONObject updatedInfo = updatedInfoArray.getJSONObject(0);
        if (updatedInfo.has("status") && 401 == updatedInfo.getInt("status")) {
          logger.error("Error code returned by IGDB.");
          logger.error("Title: " + updatedInfo.getString("title"));
          logger.error("Cause: " + updatedInfo.getString("cause"));
          logger.error("Details: " + updatedInfo.getString("details"));
          throw new IllegalStateException("Failure fetching from IGDB.");
        }
        Integer steamID = findSteamID(updatedInfo);
        String title = game.title.getValue();
        if (steamID != null) {
          logger.debug("Found Steam ID " + steamID + " for game '" + title + "'");
          game.steamID.changeValue(steamID);
          try {
            game.commit(connection);
          } catch (SQLException e) {
            logger.debug("Failed to update game '" + title + "'");
          }
        } else {
          logger.debug("No Steam ID for game '" + title + "'");
        }
      }

      //noinspection BusyWait
      Thread.sleep(250);

    }
  }

  private Integer findSteamID(JSONObject updatedInfo) {
    JSONArray websites = jsonReader.getArrayWithKey(updatedInfo, "websites");
    for (Object websiteObj : websites) {
      JSONObject website = (JSONObject) websiteObj;
      String url = jsonReader.getStringWithKey(website, "url");
      if (url.startsWith("https://store.steampowered.com")) {
        String[] split = url.split("/");
        String finalPiece = split[split.length - 1];
        try {
          return Integer.parseInt(finalPiece);
        } catch (NumberFormatException e) {
          logger.debug("Illegal ID: " + finalPiece);
          return null;
        }
      }
    }
    return null;
  }
}