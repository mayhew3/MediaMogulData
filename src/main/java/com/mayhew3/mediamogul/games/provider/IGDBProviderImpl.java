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

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class IGDBProviderImpl implements IGDBProvider {

  private String igdb_key;
  private String api_url_base = "https://api-v3.igdb.com";

  private static Logger logger = LogManager.getLogger(IGDBProviderImpl.class);

  public IGDBProviderImpl() throws MissingEnvException {
    igdb_key = EnvironmentChecker.getOrThrow("igdb_v3_key");
  }

  @Override
  public JSONArray findGameMatches(String gameTitle) {

    String gameTitleEncoded = encodeGameTitle(gameTitle);

    String url = api_url_base + "/games/";
    HashMap<String, Object> queryVars = new HashMap<>();
    queryVars.put("search", "\"" + gameTitle + "\"");
    queryVars.put("fields", "age_ratings,aggregated_rating,aggregated_rating_count,alternative_names,artworks,bundles,category,collection,cover,created_at,dlcs,expansions,external_games,first_release_date,follows,franchise,franchises,game_engines,game_modes,genres,hypes,involved_companies,keywords,multiplayer_modes,name,parent_game,platforms,player_perspectives,popularity,pulse_count,rating,rating_count,release_dates,screenshots,similar_games,slug,standalone_expansions,status,storyline,summary,tags,themes,time_to_beat,total_rating,total_rating_count,updated_at,url,version_parent,version_title,videos,websites");
    queryVars.put("offset", "0");
    queryVars.put("where", "version_parent = null");

    return getArrayData(url, queryVars);
  }

  @Override
  public JSONArray getUpdatedInfo(Integer igdb_id) {
    String url = api_url_base + "/games";
    HashMap<String, Object> queryVars = new HashMap<>();
    queryVars.put("where", "id = " + igdb_id);
    queryVars.put("fields", "name");

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

  private String encodeGameTitle(String gameTitle) {
    String gameTitleEncoded;
    gameTitleEncoded = gameTitle.replace(":", "");
    gameTitleEncoded = URLEncoder.encode(gameTitleEncoded, StandardCharsets.UTF_8);
    return gameTitleEncoded;
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
