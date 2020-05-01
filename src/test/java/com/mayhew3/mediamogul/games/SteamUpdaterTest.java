package com.mayhew3.mediamogul.games;

import com.mayhew3.mediamogul.ChromeProvider;
import com.mayhew3.mediamogul.DatabaseTest;
import com.mayhew3.mediamogul.exception.MissingEnvException;
import com.mayhew3.mediamogul.games.provider.IGDBProvider;
import com.mayhew3.mediamogul.games.provider.IGDBTestProviderImpl;
import com.mayhew3.mediamogul.games.provider.SteamTestProviderImpl;
import com.mayhew3.mediamogul.model.Person;
import com.mayhew3.mediamogul.model.games.*;
import com.mayhew3.mediamogul.xml.JSONReader;
import com.mayhew3.mediamogul.xml.JSONReaderImpl;
import org.junit.Test;

import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.fest.assertions.api.Assertions.assertThat;

@SuppressWarnings("OptionalGetWithoutIsPresent")
public class SteamUpdaterTest extends DatabaseTest {
  private SteamTestProviderImpl steamProvider;
  private ChromeProvider chromeProvider;
  private IGDBProvider igdbProvider;
  private int person_id;
  private JSONReader jsonReader;

  @Override
  public void setUp() throws URISyntaxException, SQLException, MissingEnvException {
    super.setUp();
    steamProvider = new SteamTestProviderImpl("src\\test\\resources\\Steam\\steam_", new JSONReaderImpl());
    chromeProvider = new ChromeProvider();
    jsonReader = new JSONReaderImpl();
    igdbProvider = new IGDBTestProviderImpl("src\\test\\resources\\IGDBTest\\", jsonReader);
    person_id = 1;
    createPerson();
  }

  @Test
  public void testNewSteamGame() throws SQLException {
    steamProvider.setFileSuffix("xcom2");
    String gameName = "XCOM 2";
    int playtime = 11558;
    int steamID = 268500;

    createOwnedGame("Clunkers", 48762, 10234, "Steam");

    SteamGameUpdateRunner steamGameUpdateRunner = new SteamGameUpdateRunner(connection, person_id, steamProvider, chromeProvider, igdbProvider, jsonReader);
    steamGameUpdateRunner.runUpdate();


    Optional<Game> optionalGame = findGameFromDB(gameName);

    assertThat(optionalGame.isPresent())
        .as("Expected game XCOM 2 to exist in database.")
        .isTrue();

    Game game = optionalGame.get();

    Optional<PersonGame> optionalPersonGame = game.getPersonGame(person_id, connection);

    assertThat(optionalPersonGame.isPresent())
        .isTrue();

    PersonGame personGame = optionalPersonGame.get();

    assertThat(game.steam_title.getValue())
        .isEqualTo(gameName);
    assertThat(game.steamID.getValue())
        .isEqualTo(268500);
    assertThat(game.icon.getValue())
        .isEqualTo("f275aeb0b1b947262810569356a199848c643754");
    assertThat(game.logo.getValue())
        .isEqualTo("10a6157d6614f63cd8a95d002d022778c207c218");
    assertThat(game.metacriticPage.getValue())
        .isFalse();

    assertThat(personGame.minutes_played.getValue())
        .isEqualTo(playtime);
    assertThat(personGame.tier.getValue())
        .isEqualTo(2);
    assertThat(personGame.last_played.getValue())
        .isNotNull();

    List<AvailableGamePlatform> availableGamePlatforms = game.getAvailableGamePlatforms(connection);
    assertThat(availableGamePlatforms)
        .hasSize(1);

    AvailableGamePlatform availableGamePlatform = availableGamePlatforms.get(0);
    assertThat(availableGamePlatform.platformName.getValue())
        .isEqualTo("Steam");
    assertThat(availableGamePlatform.gamePlatformID.getValue())
        .isNotNull();
    assertThat(availableGamePlatform.gameID.getValue())
        .isEqualTo(game.id.getValue());

    List<MyGamePlatform> myPlatforms = personGame.getMyPlatforms(connection);
    assertThat(myPlatforms)
        .hasSize(1);

    MyGamePlatform myPlatform = myPlatforms.get(0);
    assertThat(myPlatform.availableGamePlatformID.getValue())
        .isEqualTo(availableGamePlatform.id.getValue());
    assertThat(myPlatform.personID.getValue())
        .isEqualTo(person_id);
    assertThat(myPlatform.platformName.getValue())
        .isEqualTo("Steam");
    assertThat(myPlatform.minutes_played.getValue())
        .isEqualTo(playtime);
    assertThat(myPlatform.last_played.getValue())
        .isNotNull();

    List<GameLog> gameLogs = findGameLogs(game);
    assertThat(gameLogs)
        .hasSize(1);

    GameLog gameLog = gameLogs.get(0);
    assertThat(gameLog.game.getValue())
        .isEqualTo(gameName);
    assertThat(gameLog.steamID.getValue())
        .isEqualTo(steamID);
    assertThat(gameLog.platform.getValue())
        .isEqualTo("Steam");
    assertThat(gameLog.previousPlaytime.getValue())
        .isEqualByComparingTo(BigDecimal.ZERO);
    assertThat(gameLog.updatedplaytime.getValue())
        .isEqualByComparingTo(new BigDecimal(playtime));
    assertThat(gameLog.diff.getValue())
        .isEqualByComparingTo(new BigDecimal(playtime));
    assertThat(gameLog.eventdate.getValue())
        .isNotNull();
    assertThat(gameLog.eventtype.getValue())
        .isEqualTo("Played");
  }

  @Test
  public void testModifySteamGame() throws SQLException {
    steamProvider.setFileSuffix("xcom2");
    String gameName = "XCOM 2";
    int playtime = 11558;
    int steamID = 268500;

    createOwnedGame(gameName, steamID, 10234, "Steam");

    SteamGameUpdateRunner steamGameUpdateRunner = new SteamGameUpdateRunner(connection, person_id, steamProvider, chromeProvider, igdbProvider, jsonReader);
    steamGameUpdateRunner.runUpdate();


    Optional<Game> optionalGame = findGameFromDB(gameName);

    assertThat(optionalGame.isPresent())
        .as("Expected game XCOM 2 to exist in database.")
        .isTrue();

    Game game = optionalGame.get();

    Optional<PersonGame> optionalPersonGame = findPersonGame(game);

    assertThat(optionalPersonGame.isPresent())
        .isTrue();

    PersonGame personGame = optionalPersonGame.get();

    assertThat(game.steam_title.getValue())
        .isEqualTo(gameName);

    assertThat(game.steamID.getValue())
        .isEqualTo(steamID);
    assertThat(game.icon.getValue())
        .isEqualTo("f275aeb0b1b947262810569356a199848c643754");
    assertThat(game.logo.getValue())
        .isEqualTo("10a6157d6614f63cd8a95d002d022778c207c218");
    assertThat(game.metacriticPage.getValue())
        .isFalse();

    assertThat(personGame.minutes_played.getValue())
        .isEqualTo(playtime);
    assertThat(personGame.tier.getValue())
        .isEqualTo(2);

    List<GameLog> gameLogs = findGameLogs(game);
    assertThat(gameLogs)
        .hasSize(1);

    GameLog gameLog = gameLogs.get(0);
    assertThat(gameLog.game.getValue())
        .isEqualTo(gameName);
    assertThat(gameLog.steamID.getValue())
        .isEqualTo(steamID);
    assertThat(gameLog.platform.getValue())
        .isEqualTo("Steam");
    assertThat(gameLog.previousPlaytime.getValue())
        .isEqualByComparingTo(new BigDecimal(10234));
    assertThat(gameLog.updatedplaytime.getValue())
        .isEqualByComparingTo(new BigDecimal(playtime));
    assertThat(gameLog.diff.getValue())
        .isEqualByComparingTo(new BigDecimal(playtime - 10234));
    assertThat(gameLog.eventdate.getValue())
        .isNotNull();
    assertThat(gameLog.eventtype.getValue())
        .isEqualTo("Played");

  }

  @Test
  public void testAddSteamPlatformToExistingGame() throws SQLException {
    steamProvider.setFileSuffix("xcom2");
    String gameName = "XCOM 2";
    int playtime = 11558;
    int igdb_id = 10919;

    Game originalGame = createOwnedGame(gameName, null, 135, "Xbox One");
    originalGame.igdb_id.changeValue(igdb_id);
    originalGame.commit(connection);
    assertThat(originalGame.getAvailableGamePlatforms(connection))
        .hasSize(1);

    SteamGameUpdateRunner steamGameUpdateRunner = new SteamGameUpdateRunner(connection, person_id, steamProvider, chromeProvider, igdbProvider, jsonReader);
    steamGameUpdateRunner.runUpdate();

    Game game = findGameFromDB(gameName).get();

    Optional<PersonGame> optionalPersonGame = findPersonGame(game);

    assertThat(optionalPersonGame.isPresent())
        .isTrue();

    PersonGame personGame = optionalPersonGame.get();

    assertThat(game.steam_title.getValue())
        .isEqualTo(gameName);

    assertThat(personGame.minutes_played.getValue())
        .isEqualTo(playtime);
    assertThat(personGame.tier.getValue())
        .isEqualTo(2);

    List<AvailableGamePlatform> availableGamePlatforms = game.getAvailableGamePlatforms(connection);
    assertThat(availableGamePlatforms)
        .hasSize(2);

    AvailableGamePlatform availableGamePlatform = getAvailablePlatformWithName(game, "Steam");
    assertThat(availableGamePlatform.gameID.getValue())
        .isEqualTo(game.id.getValue());

    List<MyGamePlatform> myPlatforms = personGame.getMyPlatforms(connection);
    assertThat(myPlatforms)
        .hasSize(2);

    MyGamePlatform myPlatform = getMyPlatformWithName(personGame, "Steam");
    assertThat(myPlatform.availableGamePlatformID.getValue())
        .isEqualTo(availableGamePlatform.id.getValue());
    assertThat(myPlatform.personID.getValue())
        .isEqualTo(person_id);
    assertThat(myPlatform.platformName.getValue())
        .isEqualTo("Steam");
    assertThat(myPlatform.minutes_played.getValue())
        .isEqualTo(playtime);
    assertThat(myPlatform.last_played.getValue())
        .isNotNull();

  }

  @Test
  public void testAddSteamPlatformToExistingGameWithNoExactTitleMatch() throws SQLException {
    steamProvider.setFileSuffix("plerpen");
    String gameName = "Plerpen";
    int playtime = 11558;
    int igdb_id = 10919;

    Game originalGame = createOwnedGame(gameName, null, 135, "Xbox One");
    originalGame.igdb_id.changeValue(igdb_id);
    originalGame.commit(connection);

    assertThat(originalGame.getAvailableGamePlatforms(connection))
        .hasSize(1);

    SteamGameUpdateRunner steamGameUpdateRunner = new SteamGameUpdateRunner(connection, person_id, steamProvider, chromeProvider, igdbProvider, jsonReader);
    steamGameUpdateRunner.runUpdate();

    Game game = findGameFromDB(gameName).get();

    Optional<PersonGame> optionalPersonGame = findPersonGame(game);

    assertThat(optionalPersonGame.isPresent())
        .isTrue();

    PersonGame personGame = optionalPersonGame.get();

    assertThat(game.steam_title.getValue())
        .isEqualTo(gameName);

    assertThat(personGame.minutes_played.getValue())
        .isEqualTo(playtime);
    assertThat(personGame.tier.getValue())
        .isEqualTo(2);

    List<AvailableGamePlatform> availableGamePlatforms = game.getAvailableGamePlatforms(connection);
    assertThat(availableGamePlatforms)
        .hasSize(2);

    AvailableGamePlatform availableGamePlatform = getAvailablePlatformWithName(game, "Steam");
    assertThat(availableGamePlatform.gameID.getValue())
        .isEqualTo(game.id.getValue());

    List<MyGamePlatform> myPlatforms = personGame.getMyPlatforms(connection);
    assertThat(myPlatforms)
        .hasSize(2);

    MyGamePlatform myPlatform = getMyPlatformWithName(personGame, "Steam");
    assertThat(myPlatform.availableGamePlatformID.getValue())
        .isEqualTo(availableGamePlatform.id.getValue());
    assertThat(myPlatform.personID.getValue())
        .isEqualTo(person_id);
    assertThat(myPlatform.platformName.getValue())
        .isEqualTo("Steam");
    assertThat(myPlatform.minutes_played.getValue())
        .isEqualTo(playtime);
    assertThat(myPlatform.last_played.getValue())
        .isNotNull();

  }

  @Test
  public void testAddSteamPlatformToExistingGameWithNoExactMatch() throws SQLException {
    steamProvider.setFileSuffix("jollup");
    String gameName = "Jollup";
    int igdb_id = 10919;

    int originalPlaytime = 135;
    Game originalGame = createOwnedGame(gameName, null, originalPlaytime, "Xbox One");
    originalGame.igdb_id.changeValue(igdb_id);
    originalGame.commit(connection);

    assertThat(originalGame.getAvailableGamePlatforms(connection))
        .hasSize(1);

    SteamGameUpdateRunner steamGameUpdateRunner = new SteamGameUpdateRunner(connection, person_id, steamProvider, chromeProvider, igdbProvider, jsonReader);
    steamGameUpdateRunner.runUpdate();

    Game game = findGameFromDB(gameName).get();

    Optional<PersonGame> optionalPersonGame = findPersonGame(game);

    assertThat(optionalPersonGame.isPresent())
        .isTrue();

    PersonGame personGame = optionalPersonGame.get();

    assertThat(game.steam_title.getValue())
        .isNull();

    assertThat(personGame.minutes_played.getValue())
        .isEqualTo(originalPlaytime);

    List<AvailableGamePlatform> availableGamePlatforms = game.getAvailableGamePlatforms(connection);
    assertThat(availableGamePlatforms)
        .hasSize(1);

    List<MyGamePlatform> myPlatforms = personGame.getMyPlatforms(connection);
    assertThat(myPlatforms)
        .hasSize(1);
  }

  @Test
  public void testModifyDoesntChangeName() throws SQLException {
    steamProvider.setFileSuffix("xcom2");
    String steamName = "XCOM 2";
    String myName = "X-Com 2";

    int steamID = 268500;

    createOwnedGame(myName, steamID, 10234, "Steam");

    SteamGameUpdateRunner steamGameUpdateRunner = new SteamGameUpdateRunner(connection, person_id, steamProvider, chromeProvider, igdbProvider, jsonReader);
    steamGameUpdateRunner.runUpdate();

    Optional<Game> optionalGame = findGameFromDB(myName);

    assertThat(optionalGame.isPresent())
        .as("Expected game XCOM 2 to exist in database.")
        .isTrue();

    Game game = optionalGame.get();

    assertThat(game.steam_title.getValue())
        .isEqualTo(steamName);
    assertThat(game.title.getValue())
        .isEqualTo(myName);

    List<GameLog> gameLogs = findGameLogs(game);
    assertThat(gameLogs)
        .hasSize(1);

    GameLog gameLog = gameLogs.get(0);
    assertThat(gameLog.game.getValue())
        .isEqualTo(steamName);
  }

  @Test
  public void testSteamGameChangedToNotOwned() throws SQLException {
    steamProvider.setFileSuffix("xcom2");

    Game originalGame = createOwnedGame("Clunkers", 48762, 10234, "Steam");
    assertThat(originalGame.getPersonGame(person_id, connection).get().getMyPlatforms(connection))
        .hasSize(1);

    SteamGameUpdateRunner steamGameUpdateRunner = new SteamGameUpdateRunner(connection, person_id, steamProvider, chromeProvider, igdbProvider, jsonReader);
    steamGameUpdateRunner.runUpdate();


    Optional<Game> optionalGame = findGameFromDB("Clunkers");

    assertThat(optionalGame.isPresent())
        .as("Expected game Clunkers to exist in database.")
        .isTrue();

    Game game = optionalGame.get();

    Optional<PersonGame> optionalPersonGame = game.getPersonGame(person_id, connection);
    PersonGame personGame = optionalPersonGame.get();

    List<MyGamePlatform> myPlatforms = personGame.getMyPlatforms(connection);
    assertThat(myPlatforms)
        .isEmpty();

  }

  @Test
  public void testUnlinkThenLinkSteamGame() throws SQLException {
    steamProvider.setFileSuffix("xcom2");

    int originalMinutesPlayed = 987;
    int updatedMinutesPlayed = 1321;

    Game originalGame = createOwnedGame("Clunkers", 48762, originalMinutesPlayed, "Steam");
    assertThat(originalGame.getPersonGame(person_id, connection).get().getMyPlatforms(connection))
        .as("SANITY: Should be initialized with one MyGamePlatform")
        .hasSize(1);

    SteamGameUpdateRunner steamGameUpdateRunner = new SteamGameUpdateRunner(connection, person_id, steamProvider, chromeProvider, igdbProvider, jsonReader);
    steamGameUpdateRunner.runUpdate();


    Optional<Game> optionalGame = findGameFromDB("Clunkers");

    assertThat(optionalGame.isPresent())
        .as("Expected game Clunkers to exist in database.")
        .isTrue();

    Game game = optionalGame.get();

    List<AvailableGamePlatform> availablePlatforms = game.getAvailableGamePlatforms(connection);
    AvailableGamePlatform availablePlatform = availablePlatforms.get(0);

    PersonGame personGame = game.getPersonGame(person_id, connection).get();

    List<MyGamePlatform> originalMyPlatforms = personGame.getMyPlatforms(connection);
    assertThat(originalMyPlatforms)
        .isEmpty();

    steamProvider.setFileSuffix("clunkers");

    steamGameUpdateRunner.runUpdate();

    optionalGame = findGameFromDB("Clunkers");

    assertThat(optionalGame.isPresent())
        .as("Expected game Clunkers to exist in database.")
        .isTrue();

    List<MyGamePlatform> myPlatforms = personGame.getMyPlatforms(connection);

    assertThat(myPlatforms)
        .hasSize(1);

    MyGamePlatform myGamePlatform = myPlatforms.get(0);
    assertThat(myGamePlatform.availableGamePlatformID.getValue())
        .isEqualTo(availablePlatform.id.getValue());
    assertThat(myGamePlatform.minutes_played.getValue())
        .isEqualTo(updatedMinutesPlayed);

  }

  @Test
  public void testGameplaySessionMadeOnNewGame() throws SQLException {
    steamProvider.setFileSuffix("xcom2");
    String gameName = "XCOM 2";
    int playtime = 11558;

    createOwnedGame("Clunkers", 48762, 10234, "Steam");

    SteamGameUpdateRunner steamGameUpdateRunner = new SteamGameUpdateRunner(connection, person_id, steamProvider, chromeProvider, igdbProvider, jsonReader);
    steamGameUpdateRunner.runUpdate();

    SteamPlaySessionGenerator steamPlaySessionGenerator = new SteamPlaySessionGenerator(connection, person_id);
    steamPlaySessionGenerator.runUpdate();

    Optional<Game> optionalGame = findGameFromDB(gameName);

    assertThat(optionalGame.isPresent())
        .as("Expected game XCOM 2 to exist in database.")
        .isTrue();

    Game game = optionalGame.get();

    List<GameLog> gameLogs = findGameLogs(game);
    assertThat(gameLogs)
        .hasSize(1);

    GameLog gameLog = gameLogs.get(0);
    assertThat(gameLog.gameplaySessionID.getValue())
        .isNotNull();

    Optional<GameplaySession> gameplaySessionOptional = gameLog.getGameplaySession(connection);
    assertThat(gameplaySessionOptional.isPresent())
        .isTrue();

    GameplaySession gameplaySession = gameplaySessionOptional.get();
    assertThat(gameplaySession.gameID.getValue())
        .isEqualTo(game.id.getValue());
    assertThat(gameplaySession.startTime.getValue())
        .isNotNull();
    assertThat(gameplaySession.minutes.getValue())
        .isEqualTo(playtime);
    assertThat(gameplaySession.manualAdjustment.getValue())
        .isEqualTo(0);
    assertThat(gameplaySession.person_id.getValue())
        .isEqualTo(person_id);
  }

  @Test
  public void testGameplaySessionMadeOnUpdatedGame() throws SQLException {
    steamProvider.setFileSuffix("xcom2");
    String gameName = "XCOM 2";
    int playtime = 11558;
    int steamID = 268500;

    createOwnedGame(gameName, steamID, 10234, "Steam");

    SteamGameUpdateRunner steamGameUpdateRunner = new SteamGameUpdateRunner(connection, person_id, steamProvider, chromeProvider, igdbProvider, jsonReader);
    steamGameUpdateRunner.runUpdate();

    SteamPlaySessionGenerator steamPlaySessionGenerator = new SteamPlaySessionGenerator(connection, person_id);
    steamPlaySessionGenerator.runUpdate();

    Optional<Game> optionalGame = findGameFromDB(gameName);

    assertThat(optionalGame.isPresent())
        .as("Expected game XCOM 2 to exist in database.")
        .isTrue();

    Game game = optionalGame.get();

    List<GameLog> gameLogs = findGameLogs(game);
    assertThat(gameLogs)
        .hasSize(1);

    GameLog gameLog = gameLogs.get(0);
    assertThat(gameLog.gameplaySessionID.getValue())
        .isNotNull();

    Optional<GameplaySession> gameplaySessionOptional = gameLog.getGameplaySession(connection);
    assertThat(gameplaySessionOptional.isPresent())
        .isTrue();

    GameplaySession gameplaySession = gameplaySessionOptional.get();
    assertThat(gameplaySession.gameID.getValue())
        .isEqualTo(game.id.getValue());
    assertThat(gameplaySession.startTime.getValue())
        .isNotNull();
    assertThat(gameplaySession.minutes.getValue())
        .isEqualTo(playtime - 10234);
    assertThat(gameplaySession.manualAdjustment.getValue())
        .isEqualTo(0);
    assertThat(gameplaySession.person_id.getValue())
        .isEqualTo(person_id);
  }

  // utility methods

  private void createPerson() throws SQLException {
    Person person = new Person();
    person.initializeForInsert();
    person.id.changeValue(person_id);
    person.email.changeValue("fake@notreal.com");
    person.firstName.changeValue("Mayhew");
    person.lastName.changeValue("Fakename");

    person.commit(connection);
  }

  private Game createOwnedGame(String gameName, Integer steamID, int minutesPlayed, String platformName) throws SQLException {
    GamePlatform platform = GamePlatform.getOrCreatePlatform(connection, platformName);

    Game game = new Game();
    game.initializeForInsert();
    game.title.changeValue(gameName);
    game.steamID.changeValue(steamID);
    game.steam_title.changeValue(platformName.equals("Steam") ? gameName : null);

    game.commit(connection);

    AvailableGamePlatform availableGamePlatform = game.getOrCreatePlatform(platform, connection);

    PersonGame personGame = new PersonGame();
    personGame.initializeForInsert();
    personGame.game_id.changeValue(game.id.getValue());
    personGame.person_id.changeValue(person_id);
    personGame.tier.changeValue(2);
    personGame.minutes_played.changeValue(minutesPlayed);

    personGame.commit(connection);

    MyGamePlatform myGamePlatform = personGame.getOrCreatePlatform(connection, availableGamePlatform);
    myGamePlatform.tier.changeValue(2);
    myGamePlatform.minutes_played.changeValue(minutesPlayed);
    myGamePlatform.commit(connection);

    return game;
  }

  private AvailableGamePlatform getAvailablePlatformWithName(Game game, String platformName) throws SQLException {
    List<AvailableGamePlatform> availableGamePlatforms = game.getAvailableGamePlatforms(connection);
    Optional<AvailableGamePlatform> maybeMatching = availableGamePlatforms.stream()
        .filter(availablePlatform -> availablePlatform.platformName.getValue().equals(platformName))
        .findFirst();
    if (maybeMatching.isPresent()) {
      return maybeMatching.get();
    } else {
      throw new IllegalStateException("No platform with name " + platformName + " found for game " + game.id.getValue());
    }
  }


  private MyGamePlatform getMyPlatformWithName(PersonGame personGame, String platformName) throws SQLException {
    List<MyGamePlatform> availableGamePlatforms = personGame.getMyPlatforms(connection);
    Optional<MyGamePlatform> maybeMatching = availableGamePlatforms.stream()
        .filter(myPlatform -> myPlatform.platformName.getValue().equals(platformName))
        .findFirst();
    if (maybeMatching.isPresent()) {
      return maybeMatching.get();
    } else {
      throw new IllegalStateException("No platform with name " + platformName + " found for personGame " + personGame.id.getValue());
    }
  }



  private Optional<Game> findGameFromDB(String gameName) throws SQLException {
    String sql = "SELECT * " +
        "FROM valid_game " +
        "WHERE title = ? ";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, gameName);

    if (resultSet.next()) {
      Game game = new Game();
      game.initializeFromDBObject(resultSet);
      return Optional.of(game);
    } else {
      return Optional.empty();
    }
  }

  private Optional<PersonGame> findPersonGame(Game game) throws SQLException {
    String sql = "SELECT * " +
        "FROM person_game " +
        "WHERE game_id = ? " +
        "AND person_id = ? " +
        "AND retired = ? ";


    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, game.id.getValue(), person_id, 0);

    if (resultSet.next()) {
      PersonGame personGame = new PersonGame();
      personGame.initializeFromDBObject(resultSet);
      return Optional.of(personGame);
    } else {
      return Optional.empty();
    }
  }

  private List<GameLog> findGameLogs(Game game) throws SQLException {
    List<GameLog> gameLogs = new ArrayList<>();

    String sql = "SELECT * " +
        "FROM game_log " +
        "WHERE game_id = ? " +
        "AND person_id = ? ";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, game.id.getValue(), person_id);

    while (resultSet.next()) {
      GameLog gameLog = new GameLog();
      gameLog.initializeFromDBObject(resultSet);
      gameLogs.add(gameLog);
    }

    return gameLogs;
  }

}
