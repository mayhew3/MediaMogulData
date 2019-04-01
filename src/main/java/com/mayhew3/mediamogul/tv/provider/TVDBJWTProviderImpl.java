package com.mayhew3.mediamogul.tv.provider;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.mashape.unirest.request.HttpRequest;
import com.mayhew3.mediamogul.ExternalServiceHandler;
import org.apache.http.auth.AuthenticationException;
import org.jetbrains.annotations.NotNull;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;


public class TVDBJWTProviderImpl implements TVDBJWTProvider {
  private String token = null;
  private ExternalServiceHandler externalServiceHandler;

  public TVDBJWTProviderImpl(ExternalServiceHandler externalServiceHandler) throws UnirestException {
    if (token == null) {
      token = getToken();
    }
    this.externalServiceHandler = externalServiceHandler;
  }


  @Override
  public JSONObject findSeriesMatches(String formattedTitle) throws UnirestException, AuthenticationException {
    Preconditions.checkState(token != null);

    String seriesUrl = "https://api.thetvdb.com/search/series";

    Map<String, Object> queryParams = Maps.newHashMap();
    queryParams.put("name", formattedTitle);

    return getData(seriesUrl, queryParams);
  }

  @Override
  public JSONObject getSeriesData(Integer tvdbSeriesId) throws UnirestException, AuthenticationException {
    Preconditions.checkState(token != null);

    String seriesUrl = "https://api.thetvdb.com/series/" + tvdbSeriesId;

    return getData(seriesUrl);
  }

  @Override
  public JSONObject getEpisodeSummaries(Integer tvdbSeriesId, Integer pageNumber) throws UnirestException, AuthenticationException {
    Preconditions.checkState(token != null);

    String seriesUrl = "https://api.thetvdb.com/series/" + tvdbSeriesId + "/episodes";

    Map<String, Object> queryParams = Maps.newHashMap();
    queryParams.put("page", pageNumber);

    return getData(seriesUrl, queryParams);
  }

  @Override
  public JSONObject getEpisodeData(Integer tvdbEpisodeId) throws UnirestException, AuthenticationException {
    Preconditions.checkState(token != null);

    String seriesUrl = "https://api.thetvdb.com/episodes/" + tvdbEpisodeId;

    return getData(seriesUrl);
  }

  @Override
  public JSONObject getPosterData(Integer tvdbId) throws UnirestException, AuthenticationException {
    Preconditions.checkState(token != null);

    String seriesUrl = "https://api.thetvdb.com/series/" + tvdbId + "/images/query";

    Map<String, Object> queryParams = Maps.newHashMap();
    queryParams.put("keyType", "poster");

    return getData(seriesUrl, queryParams);
  }

  @Override
  public JSONObject getUpdatedSeries(Timestamp fromDate) throws UnirestException, AuthenticationException {
    Preconditions.checkState(token != null);

    long epochTime = getEpochTime(fromDate);

    System.out.println("Epoch time: " + epochTime);

    String seriesUrl = "https://api.thetvdb.com/updated/query";

    Map<String, Object> queryParams = Maps.newHashMap();
    queryParams.put("fromTime", epochTime);

    return getData(seriesUrl, queryParams);
  }

  public long getEpochTime(Timestamp fromDate) {
    return fromDate.getTime() / 1000L;
  }

  public void writeSearchToFile(String formattedTitle) throws UnirestException, IOException, AuthenticationException {
    Preconditions.checkState(token != null);

    String seriesUrl = "https://api.thetvdb.com/search/series";

    Map<String, Object> queryParams = Maps.newHashMap();
    queryParams.put("name", formattedTitle);

    JSONObject jsonObject = getData(seriesUrl, queryParams);

    String filePath = "src\\test\\resources\\TVDBTest\\search_" + formattedTitle + ".json";

    writeResultToFile(filePath, jsonObject);
  }

  public void writeSeriesToFile(Integer tvdbId) throws UnirestException, IOException, AuthenticationException {
    Preconditions.checkState(token != null);

    String seriesUrl = "https://api.thetvdb.com/series/" + tvdbId;

    JSONObject jsonObject = getData(seriesUrl);

    String filePath = "src\\test\\resources\\TVDBTest\\" + tvdbId + "_summary.json";

    writeResultToFile(filePath, jsonObject);
  }

  public void writePostersToFile(Integer tvdbSeriesId) throws UnirestException, IOException, AuthenticationException {
    Preconditions.checkState(token != null);

    String seriesUrl = "https://api.thetvdb.com/series/" + tvdbSeriesId + "/images/query";

    Map<String, Object> queryParams = Maps.newHashMap();
    queryParams.put("keyType", "poster");

    JSONObject jsonObject = getData(seriesUrl, queryParams);

    String filePath = "src\\test\\resources\\TVDBTest\\" + tvdbSeriesId + "_posters.json";

    writeResultToFile(filePath, jsonObject);
  }


  public void writeEpisodeDetailsToFiles(Integer tvdbSeriesId, List<EpisodeDetail> episodeNumbers) throws IOException, UnirestException, AuthenticationException {
    for (EpisodeDetail episodeNumber : episodeNumbers) {
      writeEpisodeDetailToFile(tvdbSeriesId, episodeNumber.seriesNumber, episodeNumber.episodeNumber);
    }
  }

  private void writeEpisodeDetailToFile(Integer tvdbSeriesId, Integer seasonNumber, Integer episodeNumber) throws UnirestException, IOException, AuthenticationException {
    Preconditions.checkState(token != null);

    String seriesUrl = "https://api.thetvdb.com/series/" + tvdbSeriesId + "/episodes/query";
    Map<String, Object> params = Maps.newHashMap();
    params.put("airedSeason", seasonNumber);
    params.put("airedEpisode", episodeNumber);

    JSONObject jsonObject = getData(seriesUrl, params);

    JSONArray jsonArray = jsonObject.getJSONArray("data");
    JSONObject episodeSummary = jsonArray.getJSONObject(0);

    int tvdbEpisodeId = episodeSummary.getInt("id");

    String episodeUrl = "https://api.thetvdb.com/episodes/" + tvdbEpisodeId;

    JSONObject episodeObject = getData(episodeUrl);

    String filePath = "src\\test\\resources\\TVDBTest\\" + "E" + tvdbEpisodeId + ".json";
    writeResultToFile(filePath, episodeObject);
  }


  @SuppressWarnings("ResultOfMethodCallIgnored")
  private void writeResultToFile(String localFilePath, JSONObject jsonObject) throws IOException {
    File file = new File(localFilePath);

    if (!file.exists()) {
      file.createNewFile();
    }

    FileWriter fileWriter = new FileWriter(file);
    fileWriter.write(jsonObject.toString(2));
    fileWriter.close();
  }


  private String getToken() throws UnirestException {
    String tvdbApiKey = System.getenv("TVDB_API_KEY");
    if (tvdbApiKey == null) {
      throw new IllegalStateException("No TVDB_API_KEY environment variable found!");
    }

    String urlString = "https://api.thetvdb.com/login";
    HttpRequest httpRequest = Unirest.post(urlString)
        .header("Content-Type", "application/json")
        .header("Accept", "application/json")
        .body(new JSONObject().put("apikey", tvdbApiKey))
        .getHttpRequest();

    HttpResponse<String> responseAsString = httpRequest.asString();

    return parseResponse(responseAsString);
  }

  private String parseResponse(HttpResponse<String> responseAsString) throws UnirestException {
    try {
      JSONObject jsonObject = new JSONObject(responseAsString.getBody());

      if (jsonObject.has("token")) {
        return jsonObject.getString("token");
      } else {
        System.out.println("Error fetching token. Response: ");
        System.out.println(responseAsString.getBody());
        throw new UnirestException("Unable to fetch token.");
      }

    } catch (JSONException e) {
      System.out.println("Unable to parse JSON response: ");
      System.out.println(responseAsString.getBody());
      throw e;
    }
  }

  private HttpResponse<String> getStringData(String url, Map<String, Object> queryParams) throws UnirestException, AuthenticationException {
    HttpResponse<String> response = getDataInternal(url, queryParams);

    if (response.getStatus() == 200) {
      externalServiceHandler.connectionSuccess();
      return response;
    } else if ("Unauthorized".equals(response.getStatusText())) {
      System.out.println("Refreshing token...");

      token = getToken();
      response = getDataInternal(url, queryParams);

      if ("Unauthorized".equals(response.getStatusText())) {
        externalServiceHandler.connectionFailed();
        throw new AuthenticationException("Invalid authentication.");
      } else {
        externalServiceHandler.connectionSuccess();
        return response;
      }
    } else {
      externalServiceHandler.connectionFailed();
      throw new RuntimeException("Unexpected response code: " + response.getStatus());
    }
  }

  private HttpResponse<String> getDataInternal(String url, Map<String, Object> queryParams) throws UnirestException {
    return Unirest.get(url)
        .header("Content-Type", "application/json")
        .header("Accept", "application/json")
        .header("Authorization", "Bearer " + token)
        .header("Accept-Language", "en")
        .queryString(queryParams)
        .asString();
  }

  private JSONObject getData(String url) throws UnirestException, AuthenticationException {
    HttpResponse<String> stringData = getStringData(url);
    return getJsonObject(stringData);
  }

  @NotNull
  private JSONObject getJsonObject(HttpResponse<String> stringData) {
    String body = stringData.getBody();
    try {
      return new JSONObject(body);
    } catch (JSONException e) {
      System.out.println("Unable to parse response: ");
      System.out.println(body);
      throw e;
    }
  }

  private JSONObject getData(String url, Map<String, Object> queryParams) throws UnirestException, AuthenticationException {
    HttpResponse<String> stringData = getStringData(url, queryParams);
    return getJsonObject(stringData);
  }

  private HttpResponse<String> getStringData(String url) throws UnirestException, AuthenticationException {
    return getStringData(url, Maps.newHashMap());
  }

}

