package com.mayhew3.mediamogul.games;

import com.google.common.collect.Lists;
import com.mayhew3.mediamogul.EnvironmentChecker;
import com.mayhew3.mediamogul.exception.MissingEnvException;
import com.mayhew3.mediamogul.model.games.Game;
import com.mayhew3.mediamogul.scheduler.UpdateRunner;
import com.mayhew3.mediamogul.tv.helper.UpdateMode;
import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import java.io.*;
import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

public class GiantBombUpdateRunner implements UpdateRunner {

  private SQLConnection connection;
  private String api_key;

  private static Logger logger = LogManager.getLogger(GiantBombUpdateRunner.class);

  public GiantBombUpdateRunner(SQLConnection connection) throws MissingEnvException {
    this.connection = connection;
    api_key = EnvironmentChecker.getOrThrow("giantbomb_api");
  }

  public static void main(String[] args) throws SQLException, FileNotFoundException, URISyntaxException, InterruptedException, MissingEnvException {
    List<String> argList = Lists.newArrayList(args);
    boolean singleGame = argList.contains("SingleGame");
    boolean logToFile = argList.contains("LogToFile");
    ArgumentChecker argumentChecker = new ArgumentChecker(args);

    if (logToFile) {
      String mediaMogulLogs = EnvironmentChecker.getOrThrow("MediaMogulLogs");

      File file = new File(mediaMogulLogs + "\\SteamUpdaterErrors.log");
      FileOutputStream fos = new FileOutputStream(file, true);
      PrintStream ps = new PrintStream(fos);
      System.setErr(ps);

      System.err.println("Starting run on " + new Date());
    }

    GiantBombUpdateRunner giantBombUpdateRunner = new GiantBombUpdateRunner(PostgresConnectionFactory.createConnection(argumentChecker));

    if (singleGame) {
      giantBombUpdateRunner.updateFieldsOnSingle();
    } else {
      giantBombUpdateRunner.runUpdate();
    }
  }



  private void updateFieldsOnSingle() throws SQLException, InterruptedException {
    String singleGame = "The Legend of Zelda: Breath of the Wild";

    String sql = "SELECT * FROM valid_game WHERE title = ?";
    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, singleGame);

    runUpdateOnResultSet(resultSet);

    logger.debug("Operation finished!");

  }


  public void runUpdate() throws SQLException, InterruptedException {
    String sql = "SELECT * " +
        "FROM valid_game " +
        "WHERE NOT (giantbomb_id IS NOT NULL and giantbomb_icon_url IS NOT NULL) " +
        "and owned <> 'not owned'";
    ResultSet resultSet = connection.executeQuery(sql);

    runUpdateOnResultSet(resultSet);

    logger.debug("Operation finished!");

  }

  private void runUpdateOnResultSet(ResultSet resultSet) throws SQLException, InterruptedException {
    while (resultSet.next()) {
      Game game = new Game();
      game.initializeFromDBObject(resultSet);

      GiantBombUpdater giantBombUpdater = new GiantBombUpdater(game, connection, api_key);
      giantBombUpdater.updateGame();
    }
  }

  @Override
  public String getRunnerName() {
    return "Giant Bomb Updater";
  }

  @Override
  public @Nullable UpdateMode getUpdateMode() {
    return null;
  }
}
