package com.mayhew3.mediamogul.games;

import com.mayhew3.mediamogul.DatabaseTest;
import com.mayhew3.mediamogul.exception.MissingEnvException;
import com.mayhew3.mediamogul.games.provider.IGDBProvider;
import com.mayhew3.mediamogul.games.provider.IGDBTestProviderImpl;
import com.mayhew3.mediamogul.model.games.Game;
import com.mayhew3.mediamogul.model.games.IGDBPoster;
import com.mayhew3.mediamogul.model.games.PossibleGameMatch;
import com.mayhew3.mediamogul.xml.JSONReader;
import com.mayhew3.mediamogul.xml.JSONReaderImpl;
import org.jetbrains.annotations.NotNull;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.junit.Test;

import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.jodatime.api.Assertions.assertThat;
import static org.fest.assertions.api.Assertions.assertThat;

public class IGDBUpdaterTest extends DatabaseTest {

  private IGDBProvider igdbProvider;
  private JSONReader jsonReader;

  @Override
  public void setUp() throws URISyntaxException, SQLException, MissingEnvException {
    super.setUp();
    jsonReader = new JSONReaderImpl();
    igdbProvider = new IGDBTestProviderImpl("src\\test\\resources\\IGDBTest\\", jsonReader);
  }

  @Test
  public void testManyResultsOneExactMatch() throws SQLException {
    String gameTitle = "Forza Horizon 4";

    Game game = createGame(gameTitle, "PC");

    IGDBUpdater igdbUpdater = new IGDBUpdater(game, connection, igdbProvider, jsonReader);
    igdbUpdater.updateGame();

    assertThat(game.igdb_id.getValue())
        .isEqualTo(82090);
    assertThat(game.igdb_title.getValue())
        .isEqualTo(gameTitle);
    assertThat(game.igdb_failed.getValue())
        .isNull();
    assertThat(game.igdb_success.getValue())
        .isNotNull();

    List<PossibleGameMatch> possibleGameMatches = findPossibleGameMatches(game);
    assertThat(possibleGameMatches)
        .isEmpty();

    List<IGDBPoster> posters = game.getPosters(connection);
    assertThat(posters)
        .hasSize(1);

    IGDBPoster igdbPoster = posters.get(0);
    //noinspection SpellCheckingInspection
    assertThat(igdbPoster.image_id.getValue())
        .isEqualTo("ogznieioyzvsiok1sl2m");
    assertThat(igdbPoster.width.getValue())
        .isEqualTo(1440);
    assertThat(igdbPoster.height.getValue())
        .isEqualTo(2160);
    assertThat(igdbPoster.url.getValue())
        .isEqualTo("https://images.igdb.com/igdb/image/upload/t_720p/ogznieioyzvsiok1sl2m");
  }

  @Test
  public void testTwoResultsTwoExactMatches() throws SQLException {
    String gameTitle = "Doom";

    Game game = createGame(gameTitle, "PC");

    IGDBUpdater igdbUpdater = new IGDBUpdater(game, connection, igdbProvider, jsonReader);
    igdbUpdater.updateGame();

    assertThat(game.igdb_id.getValue())
        .isEqualTo(82090);
    assertThat(game.igdb_title.getValue())
        .isEqualTo(gameTitle);
    assertThat(game.igdb_poster.getValue())
        .isNull();
    assertThat(game.igdb_poster_w.getValue())
        .isNull();
    assertThat(game.igdb_poster_h.getValue())
        .isNull();
    assertThat(game.igdb_failed.getValue())
        .isNotNull();
    assertThat(game.igdb_success.getValue())
        .isNull();

    List<PossibleGameMatch> possibleGameMatches = findPossibleGameMatches(game);
    assertThat(possibleGameMatches)
        .hasSize(2);

  }

  @Test
  public void testNoResults() throws SQLException {
    String gameTitle = "Gorfond";

    Game game = createGame(gameTitle, "Xbox One");

    IGDBUpdater igdbUpdater = new IGDBUpdater(game, connection, igdbProvider, jsonReader);
    igdbUpdater.updateGame();

    assertThat(game.igdb_id.getValue())
        .isNull();
    assertThat(game.igdb_title.getValue())
        .isNull();
    assertThat(game.igdb_poster.getValue())
        .isNull();
    assertThat(game.igdb_poster_w.getValue())
        .isNull();
    assertThat(game.igdb_poster_h.getValue())
        .isNull();
    assertThat(game.igdb_failed.getValue())
        .isNotNull();
    assertThat(game.igdb_success.getValue())
        .isNull();

    List<PossibleGameMatch> possibleGameMatches = findPossibleGameMatches(game);
    assertThat(possibleGameMatches)
        .isEmpty();
  }


  @Test
  public void testManyResultsNoExactMatch() throws SQLException {
    String gameTitle = "Forza Horizon 4 Awesome";

    Game game = createGame(gameTitle, "Xbox One");

    IGDBUpdater igdbUpdater = new IGDBUpdater(game, connection, igdbProvider, jsonReader);
    igdbUpdater.updateGame();

    assertThat(game.igdb_id.getValue())
        .isEqualTo(82090);
    assertThat(game.igdb_title.getValue())
        .isEqualTo("Forza Horizon 4");
    assertThat(game.igdb_poster.getValue())
        .isNull();
    assertThat(game.igdb_poster_w.getValue())
        .isNull();
    assertThat(game.igdb_poster_h.getValue())
        .isNull();
    assertThat(game.igdb_failed.getValue())
        .isNotNull();
    assertThat(game.igdb_success.getValue())
        .isNull();

    List<PossibleGameMatch> possibleGameMatches = findPossibleGameMatches(game);
    assertThat(possibleGameMatches)
        .hasSize(5);

    PossibleGameMatch firstMatch = possibleGameMatches.get(0);
    assertThat(firstMatch.igdbGameTitle.getValue())
        .isEqualTo("Forza Horizon 4");
    assertThat(firstMatch.igdbGameExtId.getValue())
        .isEqualTo(82090);
    assertThat(firstMatch.poster.getValue())
        .isEqualTo("ogznieioyzvsiok1sl2m");
    assertThat(firstMatch.poster_w.getValue())
        .isEqualTo(1440);
    assertThat(firstMatch.poster_h.getValue())
        .isEqualTo(2160);
  }


  @Test
  public void testSecondExecutionDoesntDuplicatePossibleMatch() throws SQLException {
    String gameTitle = "Forza Horizon 4 Awesome";

    Game game = createGame(gameTitle, "Xbox One");

    String matchTitle = "Forza Horizon 4";
    Integer matchId = 82090;
    createExistingMatch(game, matchTitle, matchId);

    IGDBUpdater igdbUpdater = new IGDBUpdater(game, connection, igdbProvider, jsonReader);
    igdbUpdater.updateGame();

    assertThat(game.igdb_id.getValue())
        .isEqualTo(matchId);
    assertThat(game.igdb_title.getValue())
        .isEqualTo(matchTitle);
    assertThat(game.igdb_poster.getValue())
        .isNull();
    assertThat(game.igdb_poster_w.getValue())
        .isNull();
    assertThat(game.igdb_poster_h.getValue())
        .isNull();
    assertThat(game.igdb_failed.getValue())
        .isNotNull();
    assertThat(game.igdb_success.getValue())
        .isNull();

    List<PossibleGameMatch> possibleGameMatches = findPossibleGameMatches(game);
    assertThat(possibleGameMatches)
        .hasSize(5);

    List<PossibleGameMatch> matchesWithId = possibleGameMatches.stream()
        .filter(possibleGameMatch -> matchId.equals(possibleGameMatch.igdbGameExtId.getValue()))
        .collect(Collectors.toList());

    assertThat(matchesWithId)
        .hasSize(1);
  }

  @Test
  public void testIGDBHintMatches() throws SQLException {
    String gameTitle = "Zeblos";
    String hintTitle = "Forza Horizon 4";

    Game game = createGame(gameTitle, "Xbox One");

    game.igdb_hint.changeValue(hintTitle);
    game.commit(connection);

    IGDBUpdater igdbUpdater = new IGDBUpdater(game, connection, igdbProvider, jsonReader);
    igdbUpdater.updateGame();

    assertThat(game.igdb_failed.getValue())
        .isNull();
    assertThat(game.igdb_id.getValue())
        .isEqualTo(82090);
    assertThat(game.igdb_title.getValue())
        .isEqualTo(hintTitle);
    assertThat(game.igdb_success.getValue())
        .isNotNull();

    List<PossibleGameMatch> possibleGameMatches = findPossibleGameMatches(game);
    assertThat(possibleGameMatches)
        .isEmpty();

    List<IGDBPoster> posters = game.getPosters(connection);
    assertThat(posters)
        .hasSize(1);

    IGDBPoster igdbPoster = posters.get(0);
    //noinspection SpellCheckingInspection
    assertThat(igdbPoster.image_id.getValue())
        .isEqualTo("ogznieioyzvsiok1sl2m");
    assertThat(igdbPoster.width.getValue())
        .isEqualTo(1440);
    assertThat(igdbPoster.height.getValue())
        .isEqualTo(2160);
    assertThat(igdbPoster.url.getValue())
        .isEqualTo("https://images.igdb.com/igdb/image/upload/t_720p/ogznieioyzvsiok1sl2m");
  }

  @Test
  public void testIGDBHintDoesntMatch() throws SQLException {
    String gameTitle = "Farhbot";
    String hintTitle = "Zeblos";

    Game game = createGame(gameTitle, "Xbox One");

    game.igdb_hint.changeValue(hintTitle);
    game.commit(connection);

    IGDBUpdater igdbUpdater = new IGDBUpdater(game, connection, igdbProvider, jsonReader);
    igdbUpdater.updateGame();

    assertThat(game.igdb_id.getValue())
        .isEqualTo(82090);
    assertThat(game.igdb_title.getValue())
        .isEqualTo("Forza Horizon 4");
    assertThat(game.igdb_poster.getValue())
        .isNull();
    assertThat(game.igdb_poster_w.getValue())
        .isNull();
    assertThat(game.igdb_poster_h.getValue())
        .isNull();
    assertThat(game.igdb_failed.getValue())
        .isNotNull();
    assertThat(game.igdb_success.getValue())
        .isNull();

    List<PossibleGameMatch> possibleGameMatches = findPossibleGameMatches(game);
    assertThat(possibleGameMatches)
        .hasSize(5);

    PossibleGameMatch firstMatch = possibleGameMatches.get(0);
    assertThat(firstMatch.igdbGameTitle.getValue())
        .isEqualTo("Forza Horizon 4");
    assertThat(firstMatch.igdbGameExtId.getValue())
        .isEqualTo(82090);
    assertThat(firstMatch.poster.getValue())
        .isEqualTo("ogznieioyzvsiok1sl2m");
    assertThat(firstMatch.poster_w.getValue())
        .isEqualTo(1440);
    assertThat(firstMatch.poster_h.getValue())
        .isEqualTo(2160);
  }

  @Test
  public void testHowlongTitleHasExactMatch() throws SQLException {
    String gameTitle = "Fahrbot";
    String howlongTitle = "Forza Horizon 4";

    Game game = createGame(gameTitle, "Xbox One");

    game.howlong_title.changeValue(howlongTitle);
    game.commit(connection);

    IGDBUpdater igdbUpdater = new IGDBUpdater(game, connection, igdbProvider, jsonReader);
    igdbUpdater.updateGame();

    assertThat(game.igdb_failed.getValue())
        .isNotNull();
    assertThat(game.igdb_id.getValue())
        .isEqualTo(82090);
    assertThat(game.igdb_title.getValue())
        .isEqualTo("Forza Horizon 4");
    assertThat(game.igdb_poster.getValue())
        .isNull();
    assertThat(game.igdb_poster_w.getValue())
        .isNull();
    assertThat(game.igdb_poster_h.getValue())
        .isNull();
    assertThat(game.igdb_success.getValue())
        .isNull();

    List<PossibleGameMatch> possibleGameMatches = findPossibleGameMatches(game);
    assertThat(possibleGameMatches)
        .hasSize(5);

    PossibleGameMatch firstMatch = possibleGameMatches.get(0);
    assertThat(firstMatch.igdbGameTitle.getValue())
        .isEqualTo("Forza Horizon 4");
    assertThat(firstMatch.igdbGameExtId.getValue())
        .isEqualTo(82090);
    assertThat(firstMatch.poster.getValue())
        .isEqualTo("ogznieioyzvsiok1sl2m");
    assertThat(firstMatch.poster_w.getValue())
        .isEqualTo(1440);
    assertThat(firstMatch.poster_h.getValue())
        .isEqualTo(2160);
  }

  @Test
  public void testGiantBombHasNoExactMatch() throws SQLException {
    String gameTitle = "Fahrbot";
    String giantBombName = "Forza Horizon 4 Awesome";

    Game game = createGame(gameTitle, "Xbox One");

    game.giantbomb_name.changeValue(giantBombName);
    game.commit(connection);

    IGDBUpdater igdbUpdater = new IGDBUpdater(game, connection, igdbProvider, jsonReader);
    igdbUpdater.updateGame();

    assertThat(game.igdb_failed.getValue())
        .isNotNull();
    assertThat(game.igdb_id.getValue())
        .isEqualTo(82090);
    assertThat(game.igdb_title.getValue())
        .isEqualTo("Forza Horizon 4");
    assertThat(game.igdb_poster.getValue())
        .isNull();
    assertThat(game.igdb_poster_w.getValue())
        .isNull();
    assertThat(game.igdb_poster_h.getValue())
        .isNull();
    assertThat(game.igdb_success.getValue())
        .isNull();

    List<PossibleGameMatch> possibleGameMatches = findPossibleGameMatches(game);
    assertThat(possibleGameMatches)
        .hasSize(5);

    PossibleGameMatch firstMatch = possibleGameMatches.get(0);
    assertThat(firstMatch.igdbGameTitle.getValue())
        .isEqualTo("Forza Horizon 4");
    assertThat(firstMatch.igdbGameExtId.getValue())
        .isEqualTo(82090);
    assertThat(firstMatch.poster.getValue())
        .isEqualTo("ogznieioyzvsiok1sl2m");
    assertThat(firstMatch.poster_w.getValue())
        .isEqualTo(1440);
    assertThat(firstMatch.poster_h.getValue())
        .isEqualTo(2160);
  }

  @Test
  public void testSteamNameIsSameAsRealName() throws SQLException {
    String gameTitle = "Forza Horizon 4 Awesome";

    Game game = createGame(gameTitle, "Xbox One");
    game.steam_title.changeValue(gameTitle);
    game.commit(connection);

    String matchTitle = "Forza Horizon 4";
    Integer matchId = 82090;
    createExistingMatch(game, matchTitle, matchId);

    IGDBUpdater igdbUpdater = new IGDBUpdater(game, connection, igdbProvider, jsonReader);
    igdbUpdater.updateGame();

    assertThat(game.igdb_id.getValue())
        .isEqualTo(matchId);
    assertThat(game.igdb_title.getValue())
        .isEqualTo(matchTitle);
    assertThat(game.igdb_poster.getValue())
        .isNull();
    assertThat(game.igdb_poster_w.getValue())
        .isNull();
    assertThat(game.igdb_poster_h.getValue())
        .isNull();
    assertThat(game.igdb_failed.getValue())
        .isNotNull();
    assertThat(game.igdb_success.getValue())
        .isNull();

    List<PossibleGameMatch> possibleGameMatches = findPossibleGameMatches(game);
    assertThat(possibleGameMatches)
        .hasSize(5);

    List<PossibleGameMatch> matchesWithId = possibleGameMatches.stream()
        .filter(possibleGameMatch -> matchId.equals(possibleGameMatch.igdbGameExtId.getValue()))
        .collect(Collectors.toList());

    assertThat(matchesWithId)
        .hasSize(1);
  }


  @Test
  public void testUpdateFieldsOnAlreadyMatched() throws SQLException {
    DateTime startOfTest = new DateTime();

    DateTime scheduledDate = startOfTest.minusDays(7);
    DateTime lastSuccess = scheduledDate.minusDays(30);
    DateTime nextScheduledDate = scheduledDate.plusDays(30);

    String gameTitle = "Forza Horizon 4";
    Integer igdb_id = 12345;

    Game game = createGame(gameTitle, "PC");

    game.igdb_id.changeValue(igdb_id);
    game.igdb_title.changeValue("Forza Horizon Four");
    game.igdb_poster.changeValue("fake_123456");
    game.igdb_poster_w.changeValue(3);
    game.igdb_poster_h.changeValue(4);
    game.igdb_success.changeValue(lastSuccess.toDate());
    game.igdb_next_update.changeValue(scheduledDate.toDate());


    IGDBUpdater igdbUpdater = new IGDBUpdater(game, connection, igdbProvider, jsonReader);
    igdbUpdater.updateGame();

    assertThat(game.title.getValue())
        .isEqualTo(gameTitle);
    assertThat(game.igdb_id.getValue())
        .isEqualTo(igdb_id);
    assertThat(game.igdb_title.getValue())
        .isEqualTo("Quidditch");
    assertThat(game.igdb_failed.getValue())
        .isNull();
    assertThat(new DateTime(game.igdb_success.getValue()))
        .isAfterOrEqualTo(startOfTest);

    DateTime nextUpdateActual = new DateTime(game.igdb_next_update.getValue());
    assertThat(nextUpdateActual)
        .isAfterOrEqualTo(nextScheduledDate);
    assertThat(Days.daysBetween(startOfTest, nextUpdateActual).getDays())
        .isEqualTo(30);

    List<PossibleGameMatch> possibleGameMatches = findPossibleGameMatches(game);
    assertThat(possibleGameMatches)
        .isEmpty();

    List<IGDBPoster> posters = game.getPosters(connection);
    assertThat(posters)
        .hasSize(1);

    IGDBPoster igdbPoster = posters.get(0);
    //noinspection SpellCheckingInspection
    assertThat(igdbPoster.image_id.getValue())
        .isEqualTo("aqbsdjsafgkdg");
    assertThat(igdbPoster.width.getValue())
        .isEqualTo(1440);
    assertThat(igdbPoster.height.getValue())
        .isEqualTo(2160);
    assertThat(igdbPoster.url.getValue())
        .isEqualTo("https://images.igdb.com/igdb/image/upload/t_720p/aqbsdjsafgkdg");
  }

  @Test
  public void testFailedIfNoResultFromID() throws SQLException {
    DateTime startOfTest = new DateTime();

    DateTime scheduledDate = startOfTest.minusDays(7);
    DateTime lastSuccess = scheduledDate.minusDays(30);
    DateTime nextScheduledDate = scheduledDate.plusDays(30);

    String gameTitle = "Crumbles";
    Integer igdb_id = 23456;

    Game game = createGame(gameTitle, "PC");

    game.igdb_id.changeValue(igdb_id);
    game.igdb_title.changeValue("Forza Horizon Four");
    game.igdb_poster.changeValue("fake_123456");
    game.igdb_poster_w.changeValue(3);
    game.igdb_poster_h.changeValue(4);
    game.igdb_success.changeValue(lastSuccess.toDate());
    game.igdb_next_update.changeValue(scheduledDate.toDate());


    IGDBUpdater igdbUpdater = new IGDBUpdater(game, connection, igdbProvider, jsonReader);
    igdbUpdater.updateGame();

    assertThat(game.title.getValue())
        .isEqualTo(gameTitle);
    assertThat(game.igdb_id.getValue())
        .isEqualTo(igdb_id);
    assertThat(game.igdb_title.getValue())
        .isEqualTo("Forza Horizon Four");
    assertThat(game.igdb_poster.getValue())
        .isEqualTo("fake_123456");
    assertThat(game.igdb_poster_w.getValue())
        .isEqualTo(3);
    assertThat(game.igdb_poster_h.getValue())
        .isEqualTo(4);
    assertThat(game.igdb_success.getValue())
        .isNull();
    assertThat(new DateTime(game.igdb_failed.getValue()))
        .isAfterOrEqualTo(startOfTest);

    DateTime nextUpdateActual = new DateTime(game.igdb_next_update.getValue());
    assertThat(nextUpdateActual)
        .isAfterOrEqualTo(startOfTest);
    assertThat(Days.daysBetween(startOfTest, nextUpdateActual).getDays())
        .isEqualTo(1);

    List<PossibleGameMatch> possibleGameMatches = findPossibleGameMatches(game);
    assertThat(possibleGameMatches)
        .isEmpty();

    igdbUpdater.updateGame();


    assertThat(game.title.getValue())
        .isEqualTo(gameTitle);
    assertThat(game.igdb_id.getValue())
        .isEqualTo(igdb_id);
    assertThat(game.igdb_title.getValue())
        .isEqualTo("Crumbles");
    assertThat(game.igdb_failed.getValue())
        .isNull();
    assertThat(new DateTime(game.igdb_success.getValue()))
        .isAfterOrEqualTo(startOfTest);

    nextUpdateActual = new DateTime(game.igdb_next_update.getValue());
    assertThat(nextUpdateActual)
        .isAfterOrEqualTo(nextScheduledDate);
    assertThat(Days.daysBetween(startOfTest, nextUpdateActual).getDays())
        .isEqualTo(30);

    possibleGameMatches = findPossibleGameMatches(game);
    assertThat(possibleGameMatches)
        .isEmpty();

    List<IGDBPoster> posters = game.getPosters(connection);
    assertThat(posters)
        .hasSize(2);

    IGDBPoster igdbPoster = posters.get(0);
    assertThat(igdbPoster.image_id.getValue())
        .isEqualTo("haiouwnfdakjlhedw");
    assertThat(igdbPoster.width.getValue())
        .isEqualTo(1440);
    assertThat(igdbPoster.height.getValue())
        .isEqualTo(2160);
    assertThat(igdbPoster.url.getValue())
        .isEqualTo("https://images.igdb.com/igdb/image/upload/t_720p/haiouwnfdakjlhedw");
  }


  // utility methods
  private Game createGame(String gameName, @NotNull String platform) throws SQLException {
    Game game = new Game();
    game.initializeForInsert();
    game.title.changeValue(gameName);
    game.platform.changeValue(platform);

    game.commit(connection);

    return game;
  }

  private void createExistingMatch(Game game, String matchTitle, Integer matchId) throws SQLException {
    PossibleGameMatch possibleGameMatch = new PossibleGameMatch();
    possibleGameMatch.initializeForInsert();

    possibleGameMatch.gameId.changeValue(game.id.getValue());
    possibleGameMatch.igdbGameExtId.changeValue(matchId);
    possibleGameMatch.igdbGameTitle.changeValue(matchTitle);

    possibleGameMatch.commit(connection);

  }

  @NotNull
  private List<PossibleGameMatch> findPossibleGameMatches(Game game) throws SQLException {
    List<PossibleGameMatch> results = new ArrayList<>();
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(
        "SELECT * " +
            "FROM possible_game_match " +
            "WHERE game_id = ? " +
            "ORDER BY id ", game.id.getValue()
    );
    while (resultSet.next()) {
      PossibleGameMatch possibleGameMatch = new PossibleGameMatch();
      possibleGameMatch.initializeFromDBObject(resultSet);
      results.add(possibleGameMatch);
    }
    return results;
  }
}
