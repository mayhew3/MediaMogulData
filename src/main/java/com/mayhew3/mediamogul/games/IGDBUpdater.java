package com.mayhew3.mediamogul.games;

import com.google.common.collect.Lists;
import com.mayhew3.mediamogul.games.provider.IGDBProvider;
import com.mayhew3.mediamogul.model.games.*;
import com.mayhew3.mediamogul.xml.JSONReader;
import com.mayhew3.postgresobject.db.SQLConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

class IGDBUpdater {
  private final Game game;
  private final String titleToSearch;

  private final SQLConnection connection;
  private final IGDBProvider igdbProvider;
  private final JSONReader jsonReader;

  private final Set<Integer> existingGameMatches = new HashSet<>();

  private static final Logger logger = LogManager.getLogger(IGDBUpdater.class);

  IGDBUpdater(@NotNull Game game, SQLConnection connection, IGDBProvider igdbProvider, JSONReader jsonReader) {
    this.game = game;
    this.connection = connection;
    this.igdbProvider = igdbProvider;
    this.jsonReader = jsonReader;

    String hint = game.igdb_hint.getValue();
    titleToSearch = hint == null ? game.title.getValue() : hint;
  }

  void updateGame() throws SQLException {
    if (isMatched()) {
      updateAlreadyMatched();
    } else {
      tryToMatch();
    }
  }

  private void tryToMatch() throws SQLException {
    updatePossibleMatches();

    JSONArray gameMatches = igdbProvider.findGameMatches(getFormattedTitle());
    processPossibleMatches(gameMatches);
  }

  private void updateAlreadyMatched() throws SQLException {
    debug("Updating already matched game '" + game.title.getValue() + "' with igdb_id " + game.igdb_id.getValue());
    JSONArray updatedInfoArray = igdbProvider.getUpdatedInfo(game.igdb_id.getValue());
    if (updatedInfoArray.length() != 1) {
      debug("Expected exactly one match for game with igdb_id: " + game.igdb_id.getValue() + ", " +
          "but there are " + updatedInfoArray.length());
      changeToFailed();
    } else {
      debug(" - Found IGDB data matching existing ID. Updating.");
      JSONObject updatedInfo = updatedInfoArray.getJSONObject(0);
      if (updatedInfo.has("status") && 401 == updatedInfo.getInt("status")) {
        logger.error("Error code returned by IGDB.");
        logger.error("Title: " + updatedInfo.getString("title"));
        logger.error("Cause: " + updatedInfo.getString("cause"));
        logger.error("Details: " + updatedInfo.getString("details"));
        throw new IllegalStateException("Failure fetching from IGDB.");
      }
      saveExactMatch(updatedInfo);
    }
  }

  private void changeToFailed() throws SQLException {
    game.igdb_failed.changeValue(new Date());
    game.igdb_success.changeValue(null);
    DateTime nextScheduled = new DateTime(new Date()).plusDays(1);
    game.igdb_next_update.changeValue(nextScheduled.toDate());
    game.commit(connection);
  }

  private String getFormattedTitle() {
    String formattedTitle = titleToSearch;
    formattedTitle = formattedTitle.replace("™", "");
    formattedTitle = formattedTitle.replace("®", "");
    return formattedTitle;
  }

  private void processPossibleMatches(JSONArray results) throws SQLException {
    debug("Processing game: ID " + game.id.getValue() + ", Title: " +
        " '" + game.title.getValue() + "', Formatted: '" + getFormattedTitle() + "'");

    Optional<JSONObject> exactMatch = findExactMatch(results);
    if (exactMatch.isPresent()) {
      debug(" - Exact match found!");
      saveExactMatch(exactMatch.get());
      incrementNextUpdate(30);
    } else {
      debug(" - No exact match.");
      List<PossibleGameMatch> possibleMatches = getPossibleMatches(results);
      tryAlternateTitles(possibleMatches);
      savePossibleMatches(possibleMatches);
      incrementNextUpdate(7);
    }
  }

  private Boolean isMatched() {
    return game.igdb_id.getValue() != null &&
        game.igdb_success.getValue() != null &&
        game.igdb_failed.getValue() == null &&
        game.igdb_ignored.getValue() == null;
  }

  private Set<String> getAlternateTitles() {
    HashSet<String> alternateTitles = new HashSet<>();
    alternateTitles.add(game.title.getValue());
    alternateTitles.add(game.howlong_title.getValue());
    alternateTitles.add(game.giantbomb_name.getValue());
    alternateTitles.add(game.steam_title.getValue());

    alternateTitles.remove(null);
    alternateTitles.remove(game.title.getValue());

    return alternateTitles;
  }

  private void tryAlternateTitles(List<PossibleGameMatch> originalMatches) {
    Set<String> alternateTitles = getAlternateTitles();
    for (String alternateTitle : alternateTitles) {
      debug(" - Getting possible matches for alternate title: '" + alternateTitle + "'");
      JSONArray gameMatches = igdbProvider.findGameMatches(alternateTitle);
      List<PossibleGameMatch> possibleMatches = getPossibleMatches(gameMatches);
      int matchCount = originalMatches.size();
      for (PossibleGameMatch possibleMatch : possibleMatches) {
        maybeAddToList(originalMatches, possibleMatch);
      }
      if (originalMatches.size() > matchCount) {
        debug(" - Found " + (originalMatches.size() - matchCount) + " additional matches.");
      }
    }
  }

  private void maybeAddToList(List<PossibleGameMatch> existingMatches, PossibleGameMatch possibleGameMatch) {
    if (!existingMatches.contains(possibleGameMatch)) {
      existingMatches.add(possibleGameMatch);
    }
  }

  private void savePossibleMatches(List<PossibleGameMatch> matches) throws SQLException {
    for (PossibleGameMatch match : matches) {
      match.commit(connection);
    }

    maybeUpdateGameWithBestMatch(matches);
    game.igdb_failed.changeValue(new Date());
    game.commit(connection);
  }

  private List<PossibleGameMatch> getPossibleMatches(JSONArray results) {
    List<PossibleGameMatch> possibleGameMatches = new ArrayList<>();

    jsonReader.forEach(results, possibleMatch -> possibleGameMatches.add(createPossibleMatch(possibleMatch)));

    return possibleGameMatches.stream().limit(5).collect(Collectors.toList());
  }

  private void maybeUpdateGameWithBestMatch(List<PossibleGameMatch> matches) {
    if (matches.size() > 0) {
      PossibleGameMatch firstMatch = matches.get(0);

      game.igdb_id.changeValue(firstMatch.igdbGameExtId.getValue());
      game.igdb_title.changeValue(firstMatch.igdbGameTitle.getValue());
    }
  }

  private Date convertFromUnixTimestamp(Integer unixTimestamp) {
    if (unixTimestamp == null) {
      return null;
    } else {
      return new java.util.Date((long)unixTimestamp*1000);
    }
  }

  private Date getEarliestReleaseDate(JSONArray release_datesJSON) {
    List<Integer> releaseDates = new ArrayList<>();
    for (Object release_dateJSON : release_datesJSON) {
      Integer releaseDate = jsonReader.getNullableIntegerWithKey((JSONObject) release_dateJSON, "date");
      releaseDates.add(releaseDate);
    }

    Optional<Integer> maybeEarliestDate = releaseDates.stream()
        .filter(Objects::nonNull)
        .min(Comparator.naturalOrder());
    return maybeEarliestDate.map(this::convertFromUnixTimestamp).orElse(null);
  }

  private void saveExactMatch(JSONObject exactMatch) throws SQLException {
    @NotNull Integer id = jsonReader.getIntegerWithKey(exactMatch, "id");
    @NotNull String name = jsonReader.getStringWithKey(exactMatch, "name");

    Double igdb_rating = jsonReader.getNullableDoubleWithKey(exactMatch, "rating");
    Integer igdb_rating_count = jsonReader.getNullableIntegerWithKey(exactMatch, "rating_count");
    Double igdb_popularity = jsonReader.getNullableDoubleWithKey(exactMatch, "popularity");
    String igdb_slug = jsonReader.getNullableStringWithKey(exactMatch, "slug");
    String igdb_summary = jsonReader.getNullableStringWithKey(exactMatch, "summary");
    Integer igdb_updated = jsonReader.getNullableIntegerWithKey(exactMatch, "updated_at");

    game.igdb_id.changeValue(id);
    game.igdb_title.changeValue(name);

    JSONArray release_datesJSON = jsonReader.getArrayWithKey(exactMatch, "release_dates");

    game.igdb_rating.changeValue(igdb_rating);
    game.igdb_rating_count.changeValue(igdb_rating_count);
    game.igdb_release_date.changeValue(getEarliestReleaseDate(release_datesJSON));
    game.igdb_popularity.changeValue(igdb_popularity);
    game.igdb_slug.changeValue(igdb_slug);
    game.igdb_summary.changeValue(igdb_summary);
    game.igdb_updated.changeValue(convertFromUnixTimestamp(igdb_updated));

    game.igdb_success.changeValue(new Date());
    game.igdb_failed.changeValue(null);

    JSONArray platformsJSON = jsonReader.getArrayWithKey(exactMatch, "platforms");

    incrementNextUpdate(30);

    if (game.id.getValue() != null) {

      updatePosters(id);
      updatePlatforms(platformsJSON, game);
      updateSteamID(game, exactMatch);

    } else {
      logger.error("Trying to update IGDB Posters on game that hasn't been committed yet: " + game.title.getValue());
    }
  }

  private void updateSteamID(Game game, JSONObject updatedInfo) {
    Integer steamID = findSteamID(updatedInfo);
    String title = game.title.getValue();
    if (steamID != null) {
      logger.debug("Found Steam ID " + steamID + " for game '" + title + "'");
      game.steamID.changeValue(steamID);
    } else {
      logger.debug("No Steam ID for game '" + title + "'");
    }
  }

  private Integer findSteamID(JSONObject updatedInfo) {
    JSONArray websites = jsonReader.getArrayWithKey(updatedInfo, "websites");
    for (Object websiteObj : websites) {
      JSONObject website = (JSONObject) websiteObj;
      String url = jsonReader.getStringWithKey(website, "url");
      if (url.startsWith("https://store.steampowered.com")) {
        String afterUrl = url.replace("https://store.steampowered.com/app/", "");
        String[] split = afterUrl.split("/");
        String firstPiece = split[0];
        try {
          return Integer.parseInt(firstPiece);
        } catch (NumberFormatException e) {
          logger.debug("Illegal ID: " + firstPiece);
          return null;
        }
      }
    }
    return null;
  }

  private void updatePlatforms(@NotNull JSONArray platforms, Game game) throws SQLException {
    List<GamePlatform> allPlatforms = GamePlatform.getAllPlatforms(connection);

    for (Object platformObj : platforms) {
      JSONObject platform = (JSONObject)platformObj;
      GamePlatform gamePlatform = getOrCreateGamePlatform(platform, allPlatforms);
      game.getOrCreatePlatform(gamePlatform, connection);
    }
  }

  private GamePlatform getOrCreateGamePlatform(@NotNull JSONObject igdbPlatform, List<GamePlatform> allPlatforms) throws SQLException {
    Integer igdbPlatformID = jsonReader.getIntegerWithKey(igdbPlatform, "id");
    String igdbPlatformName = jsonReader.getStringWithKey(igdbPlatform, "name");
    String igdbAbbreviation = jsonReader.getNullableStringWithKey(igdbPlatform, "abbreviation");

    Optional<GamePlatform> existing = allPlatforms.stream()
        .filter(gamePlatform -> igdbPlatformID.equals(gamePlatform.igdbPlatformId.getValue()))
        .findFirst();
    if (existing.isPresent()) {
      return existing.get();
    } else {
      GamePlatform gamePlatform = new GamePlatform();
      gamePlatform.initializeForInsert();
      gamePlatform.fullName.changeValue(igdbPlatformName);
      gamePlatform.shortName.changeValue(igdbAbbreviation);
      gamePlatform.igdbPlatformId.changeValue(igdbPlatformID);
      gamePlatform.igdbName.changeValue(igdbPlatformName);
      gamePlatform.commit(connection);
      return gamePlatform;
    }
  }

  private void updatePosters(@NotNull Integer igdb_id) throws SQLException {
    JSONArray covers = igdbProvider.getCovers(igdb_id);
    List<IGDBPoster> posters = new ArrayList<>();

    for (Object coverObj : covers) {
      IGDBPoster igdbPoster = updateIGDBPoster(igdb_id, (JSONObject) coverObj, game.id.getValue());
      posters.add(igdbPoster);
    }

    updateDefaultPoster(posters);
  }


  private IGDBPoster updateIGDBPoster(@NotNull Integer id, JSONObject cover, Integer game_id) throws SQLException {
    @NotNull String image_id = jsonReader.getStringWithKey(cover, "image_id");
    @NotNull String url = jsonReader.getStringWithKey(cover, "url");
    @NotNull Integer width = jsonReader.getIntegerWithKey(cover, "width");
    @NotNull Integer height = jsonReader.getIntegerWithKey(cover, "height");

    IGDBPoster igdbPoster = getOrCreateIGDBPoster(game_id, id, image_id);
    igdbPoster.igdb_game_id.changeValue(id);
    igdbPoster.image_id.changeValue(image_id);
    igdbPoster.width.changeValue(width);
    igdbPoster.height.changeValue(height);
    igdbPoster.url.changeValue(url);

    if (igdbPoster.default_for_game.getValue() == null) {
      igdbPoster.default_for_game.changeValue(false);
    }

    igdbPoster.game_id.changeValue(game.id.getValue());

    igdbPoster.commit(connection);

    return igdbPoster;
  }

  private void updateDefaultPoster(List<IGDBPoster> posters) throws SQLException {
    List<IGDBPoster> defaultPosters = posters.stream()
        .filter(igdbPoster -> igdbPoster.default_for_game.getValue())
        .collect(Collectors.toList());

    if (defaultPosters.size() == 0 && !posters.isEmpty()) {
      IGDBPoster firstPoster = posters.get(0);
      firstPoster.default_for_game.changeValue(true);
      firstPoster.commit(connection);
    } else if (defaultPosters.size() > 1) {
      throw new IllegalStateException("Found multiple default posters for game: " + game.title.getValue());
    }
  }

  private IGDBPoster getOrCreateIGDBPoster(Integer game_id, Integer igdb_game_id, String image_id) throws SQLException {
    String sql = "SELECT * " +
        "FROM igdb_poster " +
        "WHERE igdb_game_id = ? " +
        "AND game_id = ? " +
        "AND image_id = ? ";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, igdb_game_id, game_id, image_id);

    IGDBPoster igdbPoster = new IGDBPoster();

    if (resultSet.next()) {
      igdbPoster.initializeFromDBObject(resultSet);
    } else {
      igdbPoster.initializeForInsert();
    }

    return igdbPoster;
  }

  private void incrementNextUpdate(int days) throws SQLException {
    Timestamp now = new Timestamp(new Date().getTime());
    DateTime nextScheduled = new DateTime(now).plusDays(days);
    game.igdb_next_update.changeValue(nextScheduled.toDate());
    game.commit(connection);
  }

  private PossibleGameMatch createPossibleMatch(JSONObject possibleMatch) {
    @NotNull Integer id = jsonReader.getIntegerWithKey(possibleMatch, "id");

    PossibleGameMatch possibleGameMatch = getOrCreateMatch(id);

    @NotNull String name = jsonReader.getStringWithKey(possibleMatch, "name");

    possibleGameMatch.gameId.changeValue(game.id.getValue());
    possibleGameMatch.igdbGameExtId.changeValue(id);
    possibleGameMatch.igdbGameTitle.changeValue(name);

    Optional<JSONObject> maybeCover = igdbProvider.getCoverInfo(id);

    if (maybeCover.isPresent()) {
      JSONObject cover = maybeCover.get();

      @NotNull String image_id = jsonReader.getStringWithKey(cover, "image_id");
      @NotNull Integer width = jsonReader.getIntegerWithKey(cover, "width");
      @NotNull Integer height = jsonReader.getIntegerWithKey(cover, "height");

      possibleGameMatch.poster.changeValue(image_id);
      possibleGameMatch.poster_w.changeValue(width);
      possibleGameMatch.poster_h.changeValue(height);
    }

    return possibleGameMatch;
  }

  private PossibleGameMatch getOrCreateMatch(Integer igdb_id) {
    try {
      PossibleGameMatch possibleGameMatch = new PossibleGameMatch();

      if (existingGameMatches.contains(igdb_id)) {
        ResultSet resultSet = connection.prepareAndExecuteStatementFetch(
            "SELECT * " +
                "FROM possible_game_match " +
                "WHERE game_id = ? " +
                "AND igdb_game_ext_id = ? " +
                "AND retired = ? ",
            game.id.getValue(),
            igdb_id,
            0
        );

        if (resultSet.next()) {
          possibleGameMatch.initializeFromDBObject(resultSet);
        } else {
          throw new IllegalStateException("Found possible match on first pass with IGDB ID " + igdb_id + " and " +
              "Game ID " + game.id.getValue() + ", but no longer found.");
        }
      } else {
        possibleGameMatch.initializeForInsert();
      }
      return possibleGameMatch;
    } catch (SQLException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  private void updatePossibleMatches() {
    try {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(
          "SELECT igdb_game_ext_id " +
              "FROM possible_game_match " +
              "WHERE game_id = ? " +
              "AND retired = ? ", game.id.getValue(), 0);

      while (resultSet.next()) {
        Integer igdb_game_ext_id = resultSet.getInt("igdb_game_ext_id");
        existingGameMatches.add(igdb_game_ext_id);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  private Optional<JSONObject> findExactMatch(JSONArray possibleMatches) throws SQLException {
    String searchString = getFormattedTitle();

    List<JSONObject> matches = jsonReader.findMatches(possibleMatches, (possibleMatch) -> {
      String name = jsonReader.getStringWithKey(possibleMatch, "name");
      return searchString.equalsIgnoreCase(name);
    });
    if (matches.size() == 1) {
      return Optional.of(matches.get(0));
    } else if (matches.size() > 1) {
      List<String> platformNames = getPlatformNames(game);

      List<JSONObject> matchingOnPlatforms = new ArrayList<>();

      for (JSONObject matchObj : matches) {
        JSONArray platforms = matchObj.getJSONArray("platforms");
        List<String> matchPlatforms = new ArrayList<>();
        for (Object platformObj : platforms) {
          String abbreviation = jsonReader.getNullableStringWithKey((JSONObject) platformObj, "abbreviation");
          matchPlatforms.add(abbreviation);
        }
        if (matchPlatforms.containsAll(platformNames)) {
          matchingOnPlatforms.add(matchObj);
        }
      }

      if (matchingOnPlatforms.size() == 1) {
        return Optional.of(matchingOnPlatforms.get(0));
      } else {
        return Optional.empty();
      }
    } else {
      return Optional.empty();
    }
  }

  private List<String> getPlatformNames(Game game) throws SQLException {
    List<AvailableGamePlatform> availableGamePlatforms = game.getAvailableGamePlatforms(connection);
    if (availableGamePlatforms.size() == 0) {
      return Lists.newArrayList(game.platform.getValue());
    }
    return availableGamePlatforms.stream()
        .map(availableGamePlatform -> availableGamePlatform.platformName.getValue())
        .map(GamePlatform::mapInternalNameToIGDBAbbreviation)
        .distinct()
        .collect(Collectors.toList());
  }

  private static void debug(Object message) {
    logger.debug(message);
  }


}
