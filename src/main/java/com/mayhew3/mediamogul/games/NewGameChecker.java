package com.mayhew3.mediamogul.games;

import com.mayhew3.mediamogul.model.games.Game;
import com.mayhew3.mediamogul.scheduler.UpdateRunner;
import com.mayhew3.mediamogul.tv.helper.UpdateMode;
import com.mayhew3.mediamogul.tv.provider.TVDBJWTProvider;
import com.mayhew3.mediamogul.xml.JSONReader;
import com.mayhew3.postgresobject.db.SQLConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import java.sql.ResultSet;
import java.sql.SQLException;

public class NewGameChecker implements UpdateRunner {

  private SQLConnection connection;
  private TVDBJWTProvider tvdbjwtProvider;
  private JSONReader jsonReader;

  private static Logger logger = LogManager.getLogger(NewGameChecker.class);

  public NewGameChecker(SQLConnection connection, TVDBJWTProvider tvdbjwtProvider, JSONReader jsonReader) {
    this.connection = connection;
    this.tvdbjwtProvider = tvdbjwtProvider;
    this.jsonReader = jsonReader;
  }

  @Override
  public String getRunnerName() {
    return "New Game Checker";
  }

  @Override
  public @Nullable UpdateMode getUpdateMode() {
    return null;
  }

  @Override
  public void runUpdate() throws SQLException {
    String sql = "SELECT * " +
        "FROM game " +
        "WHERE first_processed = ? ";

    ResultSet resultSet = connection.prepareAndExecuteStatementFetch(sql, false);

    while (resultSet.next()) {
      Game game = new Game();
      game.initializeFromDBObject(resultSet);

      // DO STUFF
      logger.info("New Game found: '" + game.title.getValue() + "'");

      game.first_processed.changeValue(true);
      game.commit(connection);
    }
  }


}
