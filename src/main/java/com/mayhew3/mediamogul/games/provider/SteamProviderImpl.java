package com.mayhew3.mediamogul.games.provider;

import com.mayhew3.mediamogul.EnvironmentChecker;
import com.mayhew3.postgresobject.exception.MissingEnvException;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.net.URL;
import java.nio.charset.Charset;

public class SteamProviderImpl implements SteamProvider {

  private String steamKey;
  private String steamID;

  public SteamProviderImpl() throws MissingEnvException {
    steamKey = EnvironmentChecker.getOrThrow("SteamKey");
    steamID = EnvironmentChecker.getOrThrow("SteamID");
    assert steamID != null;
    assert steamKey != null;
  }

  @Override
  public JSONObject getSteamInfo() throws IOException {
    String fullUrl = getFullUrl();
    return readJsonFromUrl(fullUrl);
  }

  @Override
  public String getFullUrl() {
    return "http://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/" +
        "?key=" + steamKey +
        "&steamid=" + steamID +
        "&format=json" +
        "&include_appinfo=1";
  }

  private JSONObject readJsonFromUrl(String url) throws IOException, JSONException {
    try (InputStream is = new URL(url).openStream()) {
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
