package com.mayhew3.mediamogul.games.provider;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.mashape.unirest.request.HttpRequest;
import com.mayhew3.postgresobject.EnvironmentChecker;
import com.mayhew3.postgresobject.exception.MissingEnvException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.*;

public class IGDBProviderImpl implements IGDBProvider {

  private final String igdb_client_id;
  private final String igdb_client_secret;
  private final String api_url_base = "https://api.igdb.com/v4";

  private String token = null;

  private static final Logger logger = LogManager.getLogger(IGDBProviderImpl.class);

  public IGDBProviderImpl() throws MissingEnvException, UnirestException {
    igdb_client_id = EnvironmentChecker.getOrThrow("IGDB_V4_CLIENT_ID");
    igdb_client_secret = EnvironmentChecker.getOrThrow("IGDB_V4_CLIENT_SECRET");
    if (token == null) {
      token = getToken();
    }
  }

  private String getToken() throws UnirestException {
    String urlString = "https://id.twitch.tv/oauth2/token";
    HttpRequest httpRequest = Unirest.post(urlString)
        .header("Content-Type", "application/json")
        .header("Accept", "application/json")
        .body(new JSONObject()
            .put("client_id", igdb_client_id)
            .put("client_secret", igdb_client_secret)
            .put("grant_type", "client_credentials"))
        .getHttpRequest();

    HttpResponse<String> responseAsString = httpRequest.asString();

    return parseResponse(responseAsString);
  }

  private String parseResponse(HttpResponse<String> responseAsString) throws UnirestException {
    try {
      JSONObject jsonObject = new JSONObject(responseAsString.getBody());

      if (jsonObject.has("access_token")) {
        return jsonObject.getString("access_token");
      } else {
        debug("Error fetching access_token. Response: ");
        debug(responseAsString.getBody());
        throw new UnirestException("Unable to fetch access_token.");
      }

    } catch (JSONException e) {
      debug("Unable to parse JSON response: ");
      debug(responseAsString.getBody());
      throw e;
    }
  }

  void debug(Object message) {
    logger.debug(message);
  }

  @Override
  public JSONArray findGameMatches(String gameTitle) {
    Preconditions.checkState(token != null);

    String url = api_url_base + "/games/";
    HashMap<String, Object> queryVars = new HashMap<>();
    queryVars.put("search", "\"" + gameTitle + "\"");
    queryVars.put("fields", "name, platforms.name, platforms.abbreviation, cover.image_id, cover.width, cover.height, keywords.name, aggregated_rating, " +
        "    aggregated_rating_count, version_parent, first_release_date, genres.name, involved_companies.company.name, " +
        "    player_perspectives.name, rating, rating_count, release_dates.date, release_dates.platform.name, " +
        "    slug, summary, tags, updated_at, url, websites.url");
    queryVars.put("offset", "0");
    queryVars.put("where", "(version_parent = null & release_dates.region = (2,8))");

    return getArrayData(url, queryVars);
  }

  @Override
  public JSONArray getUpdatedInfo(Integer igdb_id) {
    Preconditions.checkState(token != null);

    String url = api_url_base + "/games";
    HashMap<String, Object> queryVars = new HashMap<>();
    queryVars.put("fields", "name, platforms.name, platforms.abbreviation, cover.image_id, cover.width, cover.height, keywords.name, aggregated_rating, " +
        "     aggregated_rating_count, version_parent, first_release_date, genres.name, involved_companies.company.name, " +
        "     player_perspectives.name, rating, rating_count, release_dates.date, release_dates.platform.name, " +
        "     slug, summary, tags, updated_at, url, websites.url");
    queryVars.put("where", "id = " + igdb_id);

    return getArrayData(url, queryVars);
  }

  public JSONArray getAllPlatforms() {
    Preconditions.checkState(token != null);

    String url = api_url_base + "/platforms";
    HashMap<String, Object> queryVars = new HashMap<>();
    queryVars.put("fields", "*");
    queryVars.put("limit", "500");
    return getArrayData(url, queryVars);
  }

  @Override
  public Optional<JSONObject> getCoverInfo(Integer game_id) {
    Preconditions.checkState(token != null);

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
    Preconditions.checkState(token != null);

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
        .header("Accept", "application/json")
        .header("Authorization", "Bearer " + token)
        .header("Client-ID", igdb_client_id)
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
