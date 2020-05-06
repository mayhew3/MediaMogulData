package com.mayhew3.mediamogul.games;

import com.mayhew3.mediamogul.model.games.*;
import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class SampleDataExporter {
  private final SQLConnection connection;

  private static final Logger logger = LogManager.getLogger(SampleDataExporter.class);

  private List<GamePlatform> allPlatforms;

  private SampleDataExporter(SQLConnection connection) {
    this.connection = connection;
  }

  public static void main(String[] args) throws URISyntaxException, SQLException, IOException {
    ArgumentChecker argumentChecker = new ArgumentChecker(args);

    SQLConnection connection = PostgresConnectionFactory.createConnection(argumentChecker);
    SampleDataExporter sampleDataExporter = new SampleDataExporter(connection);
    sampleDataExporter.runExport();
  }

  private void runExport() throws SQLException, IOException {
    updateAllPlatforms();
    JSONArray gamesJSON = new JSONArray();
    exportMyGames(gamesJSON);
    writeResultToFile("json/json_test_games.json", gamesJSON);
    exportAllPlatforms();
    exportGameplaySessions();
  }

  private void updateAllPlatforms() throws SQLException {
    this.allPlatforms = GamePlatform.getAllPlatforms(connection);
  }

  private void exportAllPlatforms() throws IOException, SQLException {
    JSONArray platformsJSON = new JSONArray();

    for (GamePlatform platform : allPlatforms) {
      JSONObject platformJSON = new JSONObject();
      platformJSON.put("id", platform.id.getValue());
      platformJSON.put("full_name", platform.fullName.getValue());
      platformJSON.put("short_name", platform.shortName.getValue());
      platformJSON.put("igdb_platform_id", platform.igdbPlatformId.getValue());
      platformJSON.put("igdb_name", platform.igdbName.getValue());
      platformJSON.put("metacritic_uri", platform.metacritic_uri.getValue());

      attachMyGlobalPlatforms(platform, platformJSON);

      platformsJSON.put(platformJSON);
    }

    writeResultToFile("json/json_test_platforms.json", platformsJSON);
  }

  private void exportGameplaySessions() throws IOException, SQLException {
    String sql = "SELECT gs.* " +
        "FROM gameplay_session gs " +
        "INNER JOIN valid_game g " +
        "  ON gs.game_id = g.id " +
        "WHERE (g.id > ? OR g.owned = ? OR g.title = ?) " +
        "ORDER BY gs.id ";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, 900, "not owned", "Portal 2");

    JSONArray sessionsArray = new JSONArray();

    while (resultSet.next()) {
      GameplaySession gameplaySession = new GameplaySession();
      gameplaySession.initializeFromDBObject(resultSet);

      JSONObject sessionObj = new JSONObject();
      sessionObj.put("id", gameplaySession.id.getValue());
      sessionObj.put("start_time", gameplaySession.startTime.getValue());
      sessionObj.put("minutes", gameplaySession.minutes.getValue());
      sessionObj.put("rating", gameplaySession.rating.getValue());
      sessionObj.put("person_id", gameplaySession.person_id.getValue());
      sessionObj.put("game_id", gameplaySession.gameID.getValue());

      sessionsArray.put(sessionObj);
    }

    writeResultToFile("json/json_test_sessions.json", sessionsArray);

    logger.info("Exported " + sessionsArray.length() + " gameplay sessions.");
  }

  private void attachMyGlobalPlatforms(GamePlatform gamePlatform, JSONObject gamePlatformObj) throws SQLException {
    String sql = "SELECT * " +
        "FROM person_platform " +
        "WHERE game_platform_id = ? ";
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, gamePlatform.id.getValue());
    JSONArray myGlobalsObj = new JSONArray();
    while (resultSet.next()) {
      PersonPlatform personPlatform = new PersonPlatform();
      personPlatform.initializeFromDBObject(resultSet);

      JSONObject myGlobalObj = new JSONObject();
      myGlobalObj.put("id", personPlatform.id.getValue());
      myGlobalObj.put("game_platform_id", personPlatform.gamePlatformID.getValue());
      myGlobalObj.put("person_id", personPlatform.personID.getValue());
      myGlobalObj.put("rank", personPlatform.rank.getValue());
      myGlobalObj.put("platform_name", personPlatform.platformName.getValue());
      myGlobalObj.put("date_added", personPlatform.dateAdded.getValue());

      myGlobalsObj.put(myGlobalObj);
    }

    gamePlatformObj.put("my_platforms", myGlobalsObj);
  }

  private void exportMyGames(JSONArray gamesJSON) throws SQLException {
    String sql = "SELECT g.* " +
        "FROM valid_game g " +
        "WHERE (g.id > ? OR g.owned = ? OR g.title = ?) " +
        "ORDER BY g.id ";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, 900, "not owned", "Portal 2");

    while (resultSet.next()) {
      Game game = new Game();
      game.initializeFromDBObject(resultSet);

      JSONObject gameJSON = new JSONObject();
      gameJSON.put("id", game.id.getValue());
      gameJSON.put("title", game.title.getValue());
      gameJSON.put("logo", game.logo.getValue());
      gameJSON.put("platform", game.platform.getValue());
      gameJSON.put("giantbomb_medium_url", game.giantbomb_medium_url.getValue());
      gameJSON.put("steamid", game.steamID.getValue());
      gameJSON.put("date_added", game.dateAdded.getValue());
      gameJSON.put("metacritic", game.metacritic.getValue());
      gameJSON.put("timetotal", game.timeTotal.getValue());
      gameJSON.put("howlong_extras", game.howlong_extras.getValue());
      gameJSON.put("natural_end", game.naturalEnd.getValue());
      gameJSON.put("metacritic_hint", game.metacriticHint.getValue());
      gameJSON.put("howlong_id", game.howlong_id.getValue());
      gameJSON.put("giantbomb_id", game.giantbomb_id.getValue());
      gameJSON.put("steam_cloud", game.steam_cloud.getValue());
      gameJSON.put("igdb_id", game.igdb_id.getValue());

      gameJSON.put("metacritic_page", game.metacriticPage.getValue());
      gameJSON.put("metacritic_matched", game.metacriticMatched.getValue());
      gameJSON.put("steam_page_gone", game.steam_page_gone.getValue());
      gameJSON.put("steam_title", game.steam_title.getValue());
      gameJSON.put("howlong_title", game.howlong_title.getValue());
      gameJSON.put("giantbomb_name", game.giantbomb_name.getValue());

      gameJSON.put("igdb_rating", game.igdb_rating.getValue());
      gameJSON.put("igdb_rating_count", game.igdb_rating_count.getValue());
      gameJSON.put("igdb_release_date", game.igdb_release_date.getValue());
      gameJSON.put("igdb_popularity", game.igdb_popularity.getValue());
      gameJSON.put("igdb_slug", game.igdb_slug.getValue());
      gameJSON.put("igdb_summary", game.igdb_summary.getValue());
      gameJSON.put("igdb_updated", game.igdb_updated.getValue());

      addPersonGamesToGame(game, gameJSON);
      attachIGDBPoster(game, gameJSON);
      addPlatformsToGame(game, gameJSON);

      gamesJSON.put(gameJSON);
    }

    logger.info("Exported " + gamesJSON.length() + " games.");
  }

  private void addPlatformsToGame(Game game, JSONObject gameJSON) throws SQLException {
    List<AvailableGamePlatform> platforms = game.getAvailableGamePlatforms(connection);
    JSONArray platformsJSON = new JSONArray();

    for (AvailableGamePlatform availablePlatform : platforms) {
      JSONObject platformJSON = new JSONObject();
      platformJSON.put("id", availablePlatform.id.getValue());
      platformJSON.put("game_platform_id", availablePlatform.gamePlatformID.getValue());
      platformJSON.put("platform_name", availablePlatform.platformName.getValue());
      platformJSON.put("metacritic", availablePlatform.metacritic.getValue());
      platformJSON.put("metacritic_page", availablePlatform.metacritic_page.getValue());
      platformJSON.put("metacritic_matched", availablePlatform.metacritic_matched.getValue());
      platformJSON.put("date_added", availablePlatform.dateAdded.getValue());

      attachMyPlatformsToAvailablePlatform(availablePlatform, platformJSON);

      platformsJSON.put(platformJSON);
    }

    gameJSON.put("availablePlatforms", platformsJSON);
  }

  private void addPersonGamesToGame(Game game, JSONObject gameJSON) throws SQLException {
    List<PersonGame> personGames = game.getPersonGames(connection);

    JSONArray personGamesJSON = new JSONArray();

    for (PersonGame personGame : personGames) {
      JSONObject personGameJSON = new JSONObject();
      personGameJSON.put("person_id", personGame.person_id.getValue());
      personGameJSON.put("last_played", personGame.last_played.getValue());
      personGameJSON.put("rating", personGame.rating.getValue());
      personGameJSON.put("tier", personGame.tier.getValue());
      personGameJSON.put("id", personGame.id.getValue());
      personGameJSON.put("final_score", personGame.final_score.getValue());
      personGameJSON.put("finished_date", personGame.finished_date.getValue());
      personGameJSON.put("replay_score", personGame.replay_score.getValue());
      personGameJSON.put("replay_reason", personGame.replay_reason.getValue());
      personGameJSON.put("date_added", personGame.dateAdded.getValue());
      personGameJSON.put("minutes_played", personGame.minutes_played.getValue());

      personGamesJSON.put(personGameJSON);
    }

    gameJSON.put("person_games", personGamesJSON);
  }

  private void attachMyPlatformsToAvailablePlatform(AvailableGamePlatform availablePlatform, JSONObject availablePlatformJSON) throws SQLException {
    List<MyGamePlatform> myPlatforms = availablePlatform.getMyPlatforms(connection);

    JSONArray myPlatformsJSON = new JSONArray();

    for (MyGamePlatform myPlatform : myPlatforms) {
      JSONObject platformJSON = new JSONObject();
      platformJSON.put("id", myPlatform.id.getValue());
      platformJSON.put("game_platform_id", availablePlatform.gamePlatformID.getValue());
      platformJSON.put("available_game_platform_id", availablePlatform.id.getValue());
      platformJSON.put("platform_name", myPlatform.platformName.getValue());
      platformJSON.put("person_id", myPlatform.personID.getValue());

      platformJSON.put("rating", myPlatform.rating.getValue());
      platformJSON.put("tier", myPlatform.tier.getValue());
      platformJSON.put("last_played", myPlatform.last_played.getValue());
      platformJSON.put("minutes_played", myPlatform.minutes_played.getValue());
      platformJSON.put("finished_date", myPlatform.finished_date.getValue());
      platformJSON.put("final_score", myPlatform.final_score.getValue());
      platformJSON.put("replay_score", myPlatform.replay_score.getValue());
      platformJSON.put("replay_reason", myPlatform.replay_reason.getValue());
      platformJSON.put("collection_add", myPlatform.collectionAdd.getValue());
      platformJSON.put("preferred", myPlatform.preferred.getValue());
      platformJSON.put("date_added", myPlatform.dateAdded.getValue());

      myPlatformsJSON.put(platformJSON);
    }

    availablePlatformJSON.put("myPlatforms", myPlatformsJSON);
  }

  private void attachIGDBPoster(Game game, JSONObject gameJSON) throws SQLException {
    String sql = "SELECT * " +
        "FROM igdb_poster " +
        "WHERE game_id = ? " +
        "AND default_for_game = ? ";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, game.id.getValue(), true);
    if (resultSet.next()) {
      IGDBPoster igdbPoster = new IGDBPoster();
      igdbPoster.initializeFromDBObject(resultSet);
      gameJSON.put("igdb_poster", igdbPoster.image_id.getValue());
      gameJSON.put("igdb_width", igdbPoster.width.getValue());
      gameJSON.put("igdb_height", igdbPoster.height.getValue());
    } else {
      gameJSON.put("igdb_poster", JSONObject.wrap(null));
    }
  }

  @SuppressWarnings({"ResultOfMethodCallIgnored", "SameParameterValue"})
  private void writeResultToFile(String localFilePath, JSONArray jsonArray) throws IOException {
    File file = new File(localFilePath);

    if (!file.exists()) {
      file.createNewFile();
    }

    FileWriter fileWriter = new FileWriter(file);
    String str = jsonArray.toString(2);

    fileWriter.write(str);
    fileWriter.close();
  }

}
