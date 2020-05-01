package com.mayhew3.mediamogul.model.games;

import com.mayhew3.postgresobject.dataobject.*;
import com.mayhew3.postgresobject.db.SQLConnection;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class Game extends RetireableDataObject {

  public FieldValueString title = registerStringField("title", Nullability.NOT_NULL);
  public FieldValueBigDecimal timeTotal = registerBigDecimalField("timetotal", Nullability.NULLABLE);
  public FieldValueBoolean naturalEnd = registerBooleanField("natural_end", Nullability.NOT_NULL).defaultValue(true);
  public FieldValueBoolean first_processed = registerBooleanField("first_processed", Nullability.NOT_NULL).defaultValue(false);


  // HOWLONG
  public FieldValueTimestamp howlong_updated = registerTimestampField("howlong_updated", Nullability.NULLABLE);
  public FieldValueTimestamp howlong_failed = registerTimestampField("howlong_failed", Nullability.NULLABLE);
  public FieldValueBigDecimal howlong_main = registerBigDecimalField("howlong_main", Nullability.NULLABLE);
  public FieldValueBigDecimal howlong_extras = registerBigDecimalField("howlong_extras", Nullability.NULLABLE);
  public FieldValueBigDecimal howlong_completionist = registerBigDecimalField("howlong_completionist", Nullability.NULLABLE);
  public FieldValueBigDecimal howlong_all = registerBigDecimalField("howlong_all", Nullability.NULLABLE);
  public FieldValueInteger howlong_id = registerIntegerField("howlong_id", Nullability.NULLABLE);
  public FieldValueInteger howlong_main_confidence = registerIntegerField("howlong_main_confidence", Nullability.NULLABLE);
  public FieldValueInteger howlong_extras_confidence = registerIntegerField("howlong_extras_confidence", Nullability.NULLABLE);
  public FieldValueInteger howlong_completionist_confidence = registerIntegerField("howlong_completionist_confidence", Nullability.NULLABLE);
  public FieldValueInteger howlong_all_confidence = registerIntegerField("howlong_all_confidence", Nullability.NULLABLE);
  public FieldValueString howlong_title = registerStringField("howlong_title", Nullability.NULLABLE);


  // STEAM
  public FieldValueInteger steamID = registerIntegerField("steamid", Nullability.NULLABLE);
  public FieldValueString icon = registerStringField("icon", Nullability.NULLABLE);
  public FieldValueString logo = registerStringField("logo", Nullability.NULLABLE);
  public FieldValueInteger steam_attribute_count = registerIntegerField("steam_attribute_count", Nullability.NULLABLE);
  public FieldValueString steam_title = registerStringField("steam_title", Nullability.NULLABLE);
  public FieldValueBoolean steam_cloud = registerBooleanFieldAllowingNulls("steam_cloud", Nullability.NULLABLE);
  public FieldValueBoolean steam_controller = registerBooleanFieldAllowingNulls("steam_controller", Nullability.NULLABLE);
  public FieldValueBoolean steam_local_coop = registerBooleanFieldAllowingNulls("steam_local_coop", Nullability.NULLABLE);
  public FieldValueTimestamp steam_attributes = registerTimestampField("steam_attributes", Nullability.NULLABLE);
  public FieldValueTimestamp steam_page_gone = registerTimestampField("steam_page_gone", Nullability.NULLABLE);


  // GIANT BOMB
  public FieldValueInteger giantbomb_id = registerIntegerField("giantbomb_id", Nullability.NULLABLE);
  public FieldValueInteger giantbomb_year = registerIntegerField("giantbomb_year", Nullability.NULLABLE);
  public FieldValueString giantbomb_name = registerStringField("giantbomb_name", Nullability.NULLABLE);
  public FieldValueString giantbomb_icon_url = registerStringField("giantbomb_icon_url", Nullability.NULLABLE);
  public FieldValueString giantbomb_medium_url = registerStringField("giantbomb_medium_url", Nullability.NULLABLE);
  public FieldValueString giantbomb_screen_url = registerStringField("giantbomb_screen_url", Nullability.NULLABLE);
  public FieldValueString giantbomb_small_url = registerStringField("giantbomb_small_url", Nullability.NULLABLE);
  public FieldValueString giantbomb_super_url = registerStringField("giantbomb_super_url", Nullability.NULLABLE);
  public FieldValueString giantbomb_thumb_url = registerStringField("giantbomb_thumb_url", Nullability.NULLABLE);
  public FieldValueString giantbomb_tiny_url = registerStringField("giantbomb_tiny_url", Nullability.NULLABLE);
  public FieldValueString giantbomb_best_guess = registerStringField("giantbomb_best_guess", Nullability.NULLABLE);
  public FieldValueString giantbomb_manual_guess = registerStringField("giantbomb_manual_guess", Nullability.NULLABLE);
  public FieldValueTimestamp giantbomb_release_date = registerTimestampField("giantbomb_release_date", Nullability.NULLABLE);
  public FieldValueBoolean giantbomb_guess_confirmed = registerBooleanFieldAllowingNulls("giantbomb_guess_confirmed", Nullability.NULLABLE);


  // IGDB
  public FieldValueInteger igdb_id = registerIntegerField("igdb_id", Nullability.NULLABLE);
  public FieldValueString igdb_title = registerStringField("igdb_title", Nullability.NULLABLE);
  public FieldValueTimestamp igdb_failed = registerTimestampField("igdb_failed", Nullability.NULLABLE);
  public FieldValueTimestamp igdb_success = registerTimestampField("igdb_success", Nullability.NULLABLE);
  public FieldValueTimestamp igdb_ignored = registerTimestampField("igdb_ignored", Nullability.NULLABLE);
  public FieldValueTimestamp igdb_next_update = registerTimestampField("igdb_next_update", Nullability.NULLABLE);
  public FieldValueString igdb_poster = registerStringField("igdb_poster", Nullability.NULLABLE);
  public FieldValueInteger igdb_poster_w = registerIntegerField("igdb_poster_w", Nullability.NULLABLE);
  public FieldValueInteger igdb_poster_h = registerIntegerField("igdb_poster_h", Nullability.NULLABLE);
  public FieldValueString igdb_hint = registerStringField("igdb_hint", Nullability.NULLABLE);

  public FieldValueBigDecimal igdb_rating = registerBigDecimalField("igdb_rating", Nullability.NULLABLE);
  public FieldValueInteger igdb_rating_count = registerIntegerField("igdb_rating_count", Nullability.NULLABLE);
  public FieldValueTimestamp igdb_release_date = registerTimestampField("igdb_release_date", Nullability.NULLABLE);
  public FieldValueBigDecimal igdb_popularity = registerBigDecimalField("igdb_popularity", Nullability.NULLABLE);
  public FieldValueString igdb_slug = registerStringField("igdb_slug", Nullability.NULLABLE);
  public FieldValueString igdb_summary = registerStringField("igdb_summary", Nullability.NULLABLE);
  public FieldValueTimestamp igdb_updated = registerTimestampField("igdb_updated", Nullability.NULLABLE);


  // METACRITIC
  public FieldValueString metacriticHint = registerStringField("metacritic_hint", Nullability.NULLABLE);


  // todo: remove once MM-116 is complete
  public FieldValueBigDecimal metacritic = registerBigDecimalField("metacritic", Nullability.NULLABLE);
  public FieldValueBoolean metacriticPage = registerBooleanField("metacritic_page", Nullability.NOT_NULL).defaultValue(false);
  public FieldValueTimestamp metacriticMatched = registerTimestampField("metacritic_matched", Nullability.NULLABLE);
  public FieldValueString platform = registerStringField("platform", Nullability.NULLABLE);
  public FieldValueString owned = registerStringField("owned", Nullability.NULLABLE);

  public Game() {
    super();
    // DB fields that aren't needed in java can be initialized in the constructor without a class member.
    addUniqueConstraint(igdb_id, retired);
  }

  @Override
  public String getTableName() {
    return "game";
  }

  @Override
  public String toString() {
    String msg = title.getValue() + " (" + id.getValue() + ")";
    Integer steamID = this.steamID.getValue();
    if (steamID != null) {
      msg += " (Steam: " + steamID + ")";
    }
    return msg;
  }

  public List<PersonGame> getPersonGames(SQLConnection connection) throws SQLException {
    String sql = "SELECT * FROM person_game " +
        "WHERE game_id = ? " +
        "AND retired = ? ";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, id.getValue(), 0);

    List<PersonGame> personGames = new ArrayList<>();

    while (resultSet.next()) {
      PersonGame personGame = new PersonGame();
      personGame.initializeFromDBObject(resultSet);
      personGames.add(personGame);
    }

    return personGames;
  }

  public Optional<PersonGame> getPersonGame(Integer person_id, SQLConnection connection) throws SQLException {
    String sql = "SELECT * FROM person_game " +
        "WHERE game_id = ? " +
        "AND person_id = ? " +
        "AND retired = ? ";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, id.getValue(), person_id, 0);

    PersonGame personGame = new PersonGame();

    if (resultSet.next()) {
      personGame.initializeFromDBObject(resultSet);
      return Optional.of(personGame);
    } else {
      return Optional.empty();
    }
  }

  public PersonGame getOrCreatePersonGame(Integer person_id, SQLConnection connection) throws SQLException {
    Optional<PersonGame> personGameOptional = getPersonGame(person_id, connection);

    if (personGameOptional.isPresent()) {
      return personGameOptional.get();
    } else {
      PersonGame personGame = new PersonGame();
      personGame.initializeForInsert();
      personGame.game_id.changeValue(id.getValue());
      personGame.person_id.changeValue(person_id);
      personGame.tier.changeValue(2);
      personGame.minutes_played.changeValue(0);
      return personGame;
    }
  }

  public List<AvailableGamePlatform> getAvailableGamePlatforms(SQLConnection connection) throws SQLException {
    String sql = "SELECT * " +
        "FROM available_game_platform " +
        "WHERE game_id = ? ";

    List<AvailableGamePlatform> platforms = new ArrayList<>();

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, id.getValue());
    while (resultSet.next()) {
      AvailableGamePlatform availableGamePlatform = new AvailableGamePlatform();
      availableGamePlatform.initializeFromDBObject(resultSet);
      platforms.add(availableGamePlatform);
    }

    return platforms;
  }

  public AvailableGamePlatform getOrCreatePlatform(GamePlatform gamePlatform, SQLConnection connection) throws SQLException {
    List<AvailableGamePlatform> availableGamePlatforms = getAvailableGamePlatforms(connection);
    Optional<AvailableGamePlatform> existing = availableGamePlatforms.stream()
        .filter(availableGamePlatform -> availableGamePlatform.gamePlatformID.getValue().equals(gamePlatform.id.getValue()))
        .findFirst();

    if (existing.isPresent()) {
      return existing.get();
    } else {
      AvailableGamePlatform availableGamePlatform = new AvailableGamePlatform();
      availableGamePlatform.initializeForInsert();
      availableGamePlatform.gameID.changeValue(id.getValue());
      availableGamePlatform.gamePlatformID.changeValue(gamePlatform.id.getValue());
      availableGamePlatform.platformName.changeValue(gamePlatform.fullName.getValue());
      availableGamePlatform.commit(connection);
      return availableGamePlatform;
    }
  }

  public List<MyGamePlatform> getAllPersonGamePlatforms(SQLConnection connection) throws SQLException {
    String sql = "SELECT mgp.* " +
        "FROM my_game_platform mgp " +
        "INNER JOIN available_game_platform agp " +
        " ON mgp.available_game_platform_id = agp.id " +
        "WHERE agp.game_id = ? ";

    List<MyGamePlatform> platforms = new ArrayList<>();

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, id.getValue());
    while (resultSet.next()) {
      MyGamePlatform myGamePlatform = new MyGamePlatform();
      myGamePlatform.initializeFromDBObject(resultSet);
      platforms.add(myGamePlatform);
    }

    return platforms;
  }

  public List<IGDBPoster> getPosters(SQLConnection connection) throws SQLException {
    String sql = "SELECT * " +
        "FROM igdb_poster " +
        "WHERE game_id = ? " +
        "ORDER BY id ";
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, id.getValue());
    List<IGDBPoster> posters = new ArrayList<>();
    while (resultSet.next()) {
      IGDBPoster igdbPoster = new IGDBPoster();
      igdbPoster.initializeFromDBObject(resultSet);
      posters.add(igdbPoster);
    }
    return posters;
  }
}
