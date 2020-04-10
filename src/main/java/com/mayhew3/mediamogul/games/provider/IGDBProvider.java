package com.mayhew3.mediamogul.games.provider;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Optional;

public interface IGDBProvider {

  JSONArray findGameMatches(String gameTitle);

  JSONArray getUpdatedInfo(Integer igdb_id);

  Optional<JSONObject> getCoverInfo(Integer igdb_cover_id);
}
