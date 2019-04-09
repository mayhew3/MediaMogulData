package com.mayhew3.mediamogul.games.provider;

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
import java.util.HashMap;
import java.util.Map;

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
    queryVars.put("search", gameTitleEncoded);
    queryVars.put("fields", "name,cover");
    queryVars.put("limit", "5");
    queryVars.put("offset", "0");

    return getArrayData(url, queryVars);
  }

  @Override
  public JSONArray getUpdatedInfo(Integer igdb_id) {
    String url = api_url_base + "/games/" + igdb_id;
    HashMap<String, Object> queryVars = new HashMap<>();
    queryVars.put("fields", "name,cover");

    return getArrayData(url, queryVars);
  }

  @Override
  public JSONObject getCoverInfo(Integer igdb_cover_id) {
    String url = api_url_base + "/covers/" + igdb_cover_id;
    HashMap<String, Object> queryVars = new HashMap<>();
    queryVars.put("fields", "image_id,width,height");

    JSONArray arrayData = getArrayData(url, queryVars);
    if (arrayData.length() == 1) {
      return arrayData.getJSONObject(0);
    } else {
      throw new IllegalStateException("No array data found for cover with id: " + igdb_cover_id);
    }
  }

  private String encodeGameTitle(String gameTitle) {
    String gameTitleEncoded;
    gameTitleEncoded = gameTitle.replace(":", "");
    gameTitleEncoded = URLEncoder.encode(gameTitleEncoded, StandardCharsets.UTF_8);
    return gameTitleEncoded;
  }


  // utilities

  private HttpResponse<String> getDataInternal(String url, Map<String, Object> queryParams) throws UnirestException {
    return Unirest.get(url)
        .header("user-key", igdb_key)
        .header("Accept", "application/json")
        .queryString(queryParams)
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
