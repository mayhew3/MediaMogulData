package com.mayhew3.mediamogul.games;

import com.mayhew3.mediamogul.DateUtils;
import com.mayhew3.mediamogul.db.DatabaseEnvironments;
import com.mayhew3.mediamogul.model.games.*;
import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.dataobject.DataObject;
import com.mayhew3.postgresobject.dataobject.FieldValue;
import com.mayhew3.postgresobject.dataobject.FieldValueTimestamp;
import com.mayhew3.postgresobject.db.DatabaseEnvironment;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
import com.mayhew3.postgresobject.exception.MissingEnvException;
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
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class SampleDataExporter {
  private final SQLConnection connection;

  private static final Logger logger = LogManager.getLogger(SampleDataExporter.class);

  private List<GamePlatform> allPlatforms;

  private SampleDataExporter(SQLConnection connection) {
    this.connection = connection;
  }

  public static void main(String[] args) throws URISyntaxException, SQLException, IOException, MissingEnvException {
    ArgumentChecker argumentChecker = new ArgumentChecker(args);

    DatabaseEnvironment environment = DatabaseEnvironments.getEnvironmentForDBArgument(argumentChecker);
    SQLConnection connection = PostgresConnectionFactory.createConnection(environment);

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
      JSONObject platformJSON = convertToJSONObject(platform, platform.dateAdded);
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

      JSONObject sessionObj = convertToJSONObject(gameplaySession,
          gameplaySession.dateAdded,
          gameplaySession.availableGamePlatform,
          gameplaySession.retired);

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

      JSONObject myGlobalObj = convertToJSONObject(personPlatform);

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

      JSONObject gameJSON = convertToJSONObject(game,
          game.giantbomb_icon_url,
          game.igdb_title,
          game.giantbomb_release_date,
          game.igdb_poster_h,
          game.howlong_all_confidence,
          game.howlong_extras_confidence,
          game.giantbomb_small_url,
          game.giantbomb_screen_url,
          game.giantbomb_id,
          game.howlong_updated,
          game.steam_local_coop,
          game.giantbomb_name,
          game.howlong_main_confidence,
          game.icon,
          game.giantbomb_year,
          game.howlong_completionist,
          game.steam_attribute_count,
          game.first_processed,
          game.steam_attributes,
          game.retired,
          game.howlong_main,
          game.howlong_all,
          game.giantbomb_thumb_url,
          game.igdb_success,
          game.igdb_next_update,
          game.howlong_failed,
          game.steam_controller,
          game.giantbomb_tiny_url,
          game.owned,
          game.howlong_completionist_confidence,
          game.giantbomb_super_url,
          game.igdb_poster_w
          );

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
      JSONObject platformJSON = convertToJSONObject(availablePlatform,
          availablePlatform.metacritic_next_update,
          availablePlatform.retired,
          availablePlatform.metacritic_failed,
          availablePlatform.gameID);

      attachMyPlatformsToAvailablePlatform(availablePlatform, platformJSON);

      platformsJSON.put(platformJSON);
    }

    gameJSON.put("availablePlatforms", platformsJSON);
  }

  private void attachMyPlatformsToAvailablePlatform(AvailableGamePlatform availablePlatform, JSONObject availablePlatformJSON) throws SQLException {
    List<MyGamePlatform> myPlatforms = availablePlatform.getMyPlatforms(connection);

    JSONArray myPlatformsJSON = new JSONArray();

    for (MyGamePlatform myPlatform : myPlatforms) {
      JSONObject platformJSON = convertToJSONObject(myPlatform);
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

  @SuppressWarnings("rawtypes")
  private JSONObject convertToJSONObject(DataObject dataObject, FieldValue ...exclusions) {
    JSONObject jsonObject = new JSONObject();
    List<FieldValue> sorted = dataObject.getAllFieldValuesIncludingId().stream()
        .sorted(Comparator.comparing(FieldValue::getFieldName))
        .collect(Collectors.toList());
    for (FieldValue exclusion : exclusions) {
      sorted.remove(exclusion);
    }
    for (FieldValue fieldValue : sorted) {
      if (fieldValue instanceof FieldValueTimestamp) {
        Timestamp value = ((FieldValueTimestamp) fieldValue).getValue();
        if (value != null) {
          LocalDateTime localDate = DateUtils.localTimeFromTimestamp(value);
          String isoDate = DateTimeFormatter.ISO_DATE_TIME.format(localDate);
          jsonObject.put(fieldValue.getFieldName(), isoDate);
        }
      } else {
        jsonObject.put(fieldValue.getFieldName(), fieldValue.getValue());
      }
    }
    return jsonObject;
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
