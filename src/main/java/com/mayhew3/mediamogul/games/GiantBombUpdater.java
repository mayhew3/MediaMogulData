package com.mayhew3.mediamogul.games;

import com.google.common.collect.Lists;
import com.mayhew3.mediamogul.model.games.Game;
import com.mayhew3.postgresobject.db.SQLConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

class GiantBombUpdater {
  private Game game;
  private SQLConnection connection;
  private String api_key;

  private static Logger logger = LogManager.getLogger(GiantBombUpdater.class);

  GiantBombUpdater(Game game, SQLConnection connection, String api_key) {
    this.game = game;
    this.connection = connection;
    this.api_key = api_key;
  }

  void updateGame() throws SQLException, InterruptedException {

    try {
      JSONObject match = findMatchIfPossible();

      if (match != null) {
        updateMatch(match);
      }
    } catch (IOException e) {
      logger.warn("Error occured for game: " + game.title.getValue());
      e.printStackTrace();
    } finally {
      // Giant Bomb added API request limits of 1 per second. Because it is exact, and I don't have many games to try,
      // giving it a buffer to not go under it.
      TimeUnit.SECONDS.sleep(2);
    }
  }


  private JSONObject findMatchIfPossible() throws IOException, SQLException {
    Integer giantbomb_id = game.giantbomb_id.getValue();
    if (giantbomb_id != null) {
      return getSingleGameWithId(giantbomb_id);
    }

    String giantbomb_title = getTitleToTry();

    JSONArray jsonArray = getResultsArray(giantbomb_title);
    JSONObject match = findMatch(jsonArray, giantbomb_title);

    if (match != null) {
      return match;
    }

    logger.debug("X) " + game.title.getValue() + ": No match found.");
    populateAlternatives(jsonArray);


    return null;
  }

  private void populateAlternatives(JSONArray originalResults) throws JSONException, IOException, SQLException {
    if (game.giantbomb_manual_guess.getValue() != null) {
      JSONArray guessResults = getResultsArray(game.giantbomb_manual_guess.getValue());
      populateBestGuess(guessResults);
    } else {
      populateBestGuess(originalResults);
    }
  }

  private void populateBestGuess(JSONArray lessRestrictive) throws SQLException {
    JSONObject bestGuess = getNextInexactResult(lessRestrictive);

    if (bestGuess != null) {
      game.giantbomb_best_guess.changeValue(bestGuess.getString("name"));
      game.commit(connection);
    }
  }


  private String getTitleToTry() {
    if (game.giantbomb_name.getValue() != null) {
      return game.giantbomb_name.getValue();
    } else if (hasConfirmedGuess()) {
      return game.giantbomb_best_guess.getValue();
    } else {
      return game.title.getValue();
    }
  }

  private boolean hasConfirmedGuess() {
    return game.giantbomb_best_guess.getValue() != null && Boolean.TRUE.equals(game.giantbomb_guess_confirmed.getValue());
  }

  private void updateMatch(@NotNull JSONObject match) throws SQLException {
    String title = game.title.getValue();
    logger.debug("O) " + title + ": Match found.");

    try {
      JSONObject image = match.getJSONObject("image");

      game.giantbomb_name.changeValue(match.getString("name"));
      game.giantbomb_id.changeValue(match.getInt("id"));

      if (match.has("original_release_date") && !match.isNull("original_release_date")) {
        String original_release_date = match.getString("original_release_date");
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Date date = simpleDateFormat.parse(original_release_date);
        Timestamp timestamp = new Timestamp(date.getTime());
        game.giantbomb_release_date.changeValue(timestamp);
        game.giantbomb_year.changeValue(new DateTime(date).getYear());
      }

      game.giantbomb_icon_url.changeValue(image.getString("icon_url"));
      game.giantbomb_medium_url.changeValue(image.getString("medium_url"));
      game.giantbomb_screen_url.changeValue(image.getString("screen_url"));
      game.giantbomb_small_url.changeValue(image.getString("small_url"));
      game.giantbomb_super_url.changeValue(image.getString("super_url"));
      game.giantbomb_thumb_url.changeValue(image.getString("thumb_url"));
      game.giantbomb_tiny_url.changeValue(image.getString("tiny_url"));

      game.commit(connection);
    } catch (JSONException e) {
      logger.warn("Error getting object for results on '" + title + "'.");
      e.printStackTrace();
    } catch (ParseException e) {
      logger.warn("Error parsing date.");
      e.printStackTrace();
    }
  }

  private JSONArray getResultsArray(String title) throws JSONException, IOException {
    String fullURL = getFullUrl(title);
    JSONObject jsonObject = readJsonFromUrl(fullURL);
    return jsonObject.getJSONArray("results");
  }

  @Nullable
  private JSONObject findMatch(JSONArray parentArray, String title) {
    List<JSONObject> matches = Lists.newArrayList();

    for (int i = 0; i < parentArray.length(); i++) {
      JSONObject jsonGame = parentArray.getJSONObject(i);

      String name = jsonGame.getString("name");

      if (title.equalsIgnoreCase(name)) {
        matches.add(jsonGame);
      }
    }

    if (matches.size() == 1) {
      return matches.get(0);
    }

    return null;
  }

  private JSONObject getNextInexactResult(JSONArray parentArray) {
    String previousBestGuess = game.giantbomb_best_guess.getValue();

    Boolean confirmed = game.giantbomb_guess_confirmed.getValue();

    if (previousBestGuess != null) {
      if (Boolean.FALSE.equals(confirmed)) {
        boolean foundPreviousGuess = false;
        for (int i = 0; i < parentArray.length(); i++) {
          JSONObject jsonGame = parentArray.getJSONObject(i);
          String name = jsonGame.getString("name");
          if (foundPreviousGuess) {
            return jsonGame;
          } else {
            if (name.equalsIgnoreCase(previousBestGuess)) {
              foundPreviousGuess = true;
            }
          }
        }
        return null;
      } else {
        return null;
      }
    }

    if (parentArray.length() == 0) {
      return null;
    } else {
      return parentArray.getJSONObject(0);
    }
  }

  private String getFullUrl(String search) {
    String encoded = URLEncoder.encode(search, StandardCharsets.UTF_8);
    return "https://www.giantbomb.com/api/search/" +
        "?api_key=" + api_key +
        "&format=json" +
        "&query=\"" + encoded + "\"" +
        "&resources=game" +
        "&field_list=id,name,image,original_release_date";
  }

  private JSONObject getSingleGameWithId(Integer id) throws IOException {
    String idUrl = getIdUrl(id);
    JSONObject jsonObject = readJsonFromUrl(idUrl);
    return jsonObject.getJSONObject("results");
  }

  private String getIdUrl(Integer id) {
    return "https://www.giantbomb.com/api/game/3030-" + id + "/" +
        "?api_key=" + api_key +
        "&format=json" +
        "&resources=game" +
        "&field_list=id,name,image,original_release_date";
  }


  private JSONObject readJsonFromUrl(String url) throws IOException, JSONException {
    URLConnection urlConnection = new URL(url).openConnection();
    urlConnection.addRequestProperty("User-Agent", "Mozilla/4.0");

    try (InputStream is = urlConnection.getInputStream()) {
      BufferedReader rd = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
      String jsonText = readAll(rd);
      return new JSONObject(jsonText);
    }
  }

  private String readAll(Reader rd) throws IOException {
    StringBuilder sb = new StringBuilder();
    int cp;
    while ((cp = rd.read()) != -1) {
      sb.append((char) cp);
    }
    return sb.toString();
  }


}
