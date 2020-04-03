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
    String sql = "SELECT pg.* " +
        "FROM game g " +
        "INNER JOIN person_game pg " +
        "  ON pg.game_id = g.id " +
        "WHERE pg.last_played > ? " +
        "AND pg.last_played IS NOT NULL " +
        "ORDER BY pg.last_played DESC";

    DateTime oneYearAgo = DateTime.now().minusMonths(36);

    Timestamp timestamp = new Timestamp(oneYearAgo.toDate().getTime());
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, timestamp);

    JSONArray gamesJSON = new JSONArray();

    while (resultSet.next()) {
      PersonGame personGame = new PersonGame();
      personGame.initializeFromDBObject(resultSet);

      JSONObject personGameJSON = new JSONObject();
      personGameJSON.put("lastPlayed", JSONObject.wrap(personGame.last_played.getValue()));
      personGameJSON.put("rating", JSONObject.wrap(personGame.rating.getValue()));
      personGameJSON.put("tier", JSONObject.wrap(personGame.tier.getValue()));
      personGameJSON.put("id", JSONObject.wrap(personGame.id.getValue()));
      personGameJSON.put("finalScore", JSONObject.wrap(personGame.final_score.getValue()));
      personGameJSON.put("finishedDate", JSONObject.wrap(personGame.finished_date.getValue()));
      personGameJSON.put("replayScore", JSONObject.wrap(personGame.replay_score.getValue()));
      personGameJSON.put("replayReason", JSONObject.wrap(personGame.replay_reason.getValue()));
      personGameJSON.put("dateAdded", JSONObject.wrap(personGame.dateAdded.getValue()));

      Game game = personGame.getGame(connection);

      JSONObject gameJSON = new JSONObject();
      gameJSON.put("id", game.id.getValue());
      gameJSON.put("title", JSONObject.wrap(game.title.getValue()));
      gameJSON.put("igdbPoster", JSONObject.wrap(game.igdb_poster.getValue()));
      gameJSON.put("logo", JSONObject.wrap(game.logo.getValue()));
      gameJSON.put("giantBombMedium", JSONObject.wrap(game.giantbomb_medium_url.getValue()));
      gameJSON.put("steamID", JSONObject.wrap(game.steamID.getValue()));
      gameJSON.put("dateAdded", JSONObject.wrap(game.dateAdded.getValue()));
      gameJSON.put("platform", JSONObject.wrap(game.platform.getValue()));
      gameJSON.put("metacritic", JSONObject.wrap(game.metacritic.getValue()));
      gameJSON.put("timeTotal", JSONObject.wrap(game.timetotal.getValue()));
      gameJSON.put("howlongExtras", JSONObject.wrap(game.howlong_extras.getValue()));
      gameJSON.put("naturalEnd", JSONObject.wrap(game.naturalEnd.getValue()));

      gameJSON.put("personGame", personGameJSON);

      gamesJSON.put(gameJSON);
    }

    logger.info("Exported " + gamesJSON.length() + " games.");

    writeResultToFile("json/json_test_games", gamesJSON);
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
