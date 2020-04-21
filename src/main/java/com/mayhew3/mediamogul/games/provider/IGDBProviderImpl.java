package com.mayhew3.mediamogul.games.provider;

import com.google.common.base.Joiner;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.mayhew3.mediamogul.EnvironmentChecker;
import com.mayhew3.mediamogul.exception.MissingEnvException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.*;

public class IGDBProviderImpl implements IGDBProvider {

  private final String igdb_key;
  private final String api_url_base = "https://api-v3.igdb.com";

  private static final Logger logger = LogManager.getLogger(IGDBProviderImpl.class);

  public IGDBProviderImpl() throws MissingEnvException {
    igdb_key = EnvironmentChecker.getOrThrow("igdb_v3_key");
  }

  @Override
  public JSONArray findGameMatches(String gameTitle) {

    String url = api_url_base + "/games/";
    HashMap<String, Object> queryVars = new HashMap<>();
    queryVars.put("search", "\"" + gameTitle + "\"");
    queryVars.put("fields", "name, platforms.name, cover.image_id, cover.width, cover.height, keywords.name, aggregated_rating, " +
        "    aggregated_rating_count, version_parent, first_release_date, genres.name, involved_companies.company.name, " +
        "    player_perspectives.name, popularity,pulse_count, rating, rating_count, release_dates.date, release_dates.platform.name, " +
        "    slug, summary, tags, updated_at, url");
    queryVars.put("offset", "0");
    queryVars.put("where", "(version_parent = null & release_dates.region = (2,8))");

    return getArrayData(url, queryVars);
  }

  @Override
  public JSONArray getUpdatedInfo(Integer igdb_id) {
    String url = api_url_base + "/games";
    HashMap<String, Object> queryVars = new HashMap<>();
    queryVars.put("fields", "name, platforms.name, cover.image_id, cover.width, cover.height, keywords.name, aggregated_rating, " +
        "    aggregated_rating_count, version_parent, first_release_date, genres.name, involved_companies.company.name, " +
        "    player_perspectives.name, popularity,pulse_count, rating, rating_count, release_dates.date, release_dates.platform.name, " +
        "    slug, summary, tags, updated_at, url");
    queryVars.put("where", "id = " + igdb_id);

    return getArrayData(url, queryVars);
  }

  @Override
  public Optional<JSONObject> getCoverInfo(Integer game_id) {
    String url = api_url_base + "/covers";
    HashMap<String, Object> queryVars = new HashMap<>();
    queryVars.put("where", "game = " + game_id);
    queryVars.put("fields", "image_id,width,height,game");

    JSONArray arrayData = getArrayData(url, queryVars);
    if (arrayData.length() > 0) {
      return Optional.of(arrayData.getJSONObject(0));
    } else {
      return Optional.empty();
    }
  }

  @Override
  public JSONArray getCovers(Integer igdb_game_id) {
    String url = api_url_base + "/covers";
    HashMap<String, Object> queryVars = new HashMap<>();
    queryVars.put("where", "game = " + igdb_game_id);
    queryVars.put("fields", "image_id,width,height,game,url");

    return getArrayData(url, queryVars);
  }


  // utilities

  private String createBodyFromParams(Map<String, Object> queryParams) {
    List<String> paramStrings = new ArrayList<>();
    for (String key : queryParams.keySet()) {
      String paramString = key + " " + queryParams.get(key) + ";";
      paramStrings.add(paramString);
    }
    return Joiner.on(" ").join(paramStrings);
  }

  private HttpResponse<String> getDataInternal(String url, Map<String, Object> queryParams) throws UnirestException {
    String body = createBodyFromParams(queryParams);
    return Unirest.post(url)
        .header("user-key", igdb_key)
        .header("Accept", "application/json")
        .body(body)
        .asString();
  }

  private JSONArray getJsonArray(HttpResponse<String> stringData) {
    String body = stringData.getBody();
    try {
      return new JSONArray(body);
    } catch (JSONException e) {
      logger.error("Unable to parse response: ");
      logger.error(body);
      throw e;
    }
  }

  private JSONArray getArrayData(String url, Map<String, Object> queryParams) {
    try {
      return getJsonArray(getDataInternal(url, queryParams));
    } catch (UnirestException e) {
      throw new RuntimeException(e);
    }
  }

  private JSONObject getJsonObject(HttpResponse<String> stringData) {
    String body = stringData.getBody();
    try {
      return new JSONObject(body);
    } catch (JSONException e) {
      logger.error("Unable to parse response: ");
      logger.error(body);
      throw e;
    }
  }

  @SuppressWarnings("unused")
  private JSONObject getObjectData(String url, Map<String, Object> queryParams) {
    try {
      HttpResponse<String> dataInternal = getDataInternal(url, queryParams);
      return getJsonObject(dataInternal);
    } catch (UnirestException e) {
      throw new RuntimeException(e);
    }
  }
}
