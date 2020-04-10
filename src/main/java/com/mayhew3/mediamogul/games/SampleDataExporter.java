package com.mayhew3.mediamogul.games;

import com.mayhew3.mediamogul.model.games.Game;
import com.mayhew3.mediamogul.model.games.PersonGame;
import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

public class SampleDataExporter {
  private SQLConnection connection;

  private static Logger logger = LogManager.getLogger(SampleDataExporter.class);

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
    JSONArray gamesJSON = new JSONArray();
    exportMyGames(gamesJSON);
    exportNotMyGames(gamesJSON);
    writeResultToFile("json/json_test_games.json", gamesJSON);
  }

  private void exportMyGames(JSONArray gamesJSON) throws SQLException {
    String sql = "SELECT pg.* " +
        "FROM game g " +
        "INNER JOIN person_game pg " +
        "  ON pg.game_id = g.id " +
        "WHERE pg.last_played > ? " +
        "AND pg.person_id = ? " +
        "AND pg.last_played IS NOT NULL " +
        "ORDER BY pg.last_played DESC";

    DateTime oneYearAgo = DateTime.now().minusMonths(36);

    Timestamp timestamp = new Timestamp(oneYearAgo.toDate().getTime());
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, timestamp, 1);

    while (resultSet.next()) {
      PersonGame personGame = new PersonGame();
      personGame.initializeFromDBObject(resultSet);

      JSONObject personGameJSON = new JSONObject();
      personGameJSON.put("last_played", JSONObject.wrap(personGame.last_played.getValue()));
      personGameJSON.put("rating", JSONObject.wrap(personGame.rating.getValue()));
      personGameJSON.put("tier", JSONObject.wrap(personGame.tier.getValue()));
      personGameJSON.put("id", JSONObject.wrap(personGame.id.getValue()));
      personGameJSON.put("final_score", JSONObject.wrap(personGame.final_score.getValue()));
      personGameJSON.put("finished_date", JSONObject.wrap(personGame.finished_date.getValue()));
      personGameJSON.put("replay_score", JSONObject.wrap(personGame.replay_score.getValue()));
      personGameJSON.put("replay_reason", JSONObject.wrap(personGame.replay_reason.getValue()));
      personGameJSON.put("date_added", JSONObject.wrap(personGame.dateAdded.getValue()));
      personGameJSON.put("minutes_played", JSONObject.wrap(personGame.minutes_played.getValue()));

      Game game = personGame.getGame(connection);

      JSONObject gameJSON = new JSONObject();
      gameJSON.put("id", game.id.getValue());
      gameJSON.put("title", JSONObject.wrap(game.title.getValue()));
      gameJSON.put("igdb_poster", JSONObject.wrap(game.igdb_poster.getValue()));
      gameJSON.put("logo", JSONObject.wrap(game.logo.getValue()));
      gameJSON.put("giantbomb_medium_url", JSONObject.wrap(game.giantbomb_medium_url.getValue()));
      gameJSON.put("steamid", JSONObject.wrap(game.steamID.getValue()));
      gameJSON.put("date_added", JSONObject.wrap(game.dateAdded.getValue()));
      gameJSON.put("platform", JSONObject.wrap(game.platform.getValue()));
      gameJSON.put("metacritic", JSONObject.wrap(game.metacritic.getValue()));
      gameJSON.put("timetotal", JSONObject.wrap(game.timetotal.getValue()));
      gameJSON.put("howlong_extras", JSONObject.wrap(game.howlong_extras.getValue()));
      gameJSON.put("natural_end", JSONObject.wrap(game.naturalEnd.getValue()));
      gameJSON.put("metacritic_hint", JSONObject.wrap(game.metacriticHint.getValue()));
      gameJSON.put("howlong_id", JSONObject.wrap(game.howlong_id.getValue()));
      gameJSON.put("giantbomb_id", JSONObject.wrap(game.giantbomb_id.getValue()));

      gameJSON.put("personGame", personGameJSON);

      gamesJSON.put(gameJSON);
    }

    logger.info("Exported " + gamesJSON.length() + " games.");
  }

  private void exportNotMyGames(JSONArray gamesJSON) throws SQLException {
    String sql = "SELECT g.* " +
        "FROM game g " +
        "WHERE g.id NOT IN (SELECT game_id FROM person_game WHERE person_id = ?) " +
        "ORDER BY g.date_added DESC ";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, 1);

    while (resultSet.next()) {
      Game game = new Game();
      game.initializeFromDBObject(resultSet);

      JSONObject gameJSON = new JSONObject();
      gameJSON.put("id", game.id.getValue());
      gameJSON.put("title", JSONObject.wrap(game.title.getValue()));
      gameJSON.put("igdb_poster", JSONObject.wrap(game.igdb_poster.getValue()));
      gameJSON.put("logo", JSONObject.wrap(game.logo.getValue()));
      gameJSON.put("giantbomb_medium_url", JSONObject.wrap(game.giantbomb_medium_url.getValue()));
      gameJSON.put("steamid", JSONObject.wrap(game.steamID.getValue()));
      gameJSON.put("date_added", JSONObject.wrap(game.dateAdded.getValue()));
      gameJSON.put("platform", JSONObject.wrap(game.platform.getValue()));
      gameJSON.put("metacritic", JSONObject.wrap(game.metacritic.getValue()));
      gameJSON.put("timetotal", JSONObject.wrap(game.timetotal.getValue()));
      gameJSON.put("howlong_extras", JSONObject.wrap(game.howlong_extras.getValue()));
      gameJSON.put("natural_end", JSONObject.wrap(game.naturalEnd.getValue()));
      gameJSON.put("metacritic_hint", JSONObject.wrap(game.metacriticHint.getValue()));
      gameJSON.put("howlong_id", JSONObject.wrap(game.howlong_id.getValue()));
      gameJSON.put("giantbomb_id", JSONObject.wrap(game.giantbomb_id.getValue()));

      gamesJSON.put(gameJSON);
    }

    logger.info("Exported " + gamesJSON.length() + " unowned games.");

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
