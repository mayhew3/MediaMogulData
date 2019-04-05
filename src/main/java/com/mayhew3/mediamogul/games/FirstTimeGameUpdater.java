package com.mayhew3.mediamogul.games;

import com.mayhew3.mediamogul.model.games.Game;
import com.mayhew3.postgresobject.db.SQLConnection;

public class FirstTimeGameUpdater {
  private Game game;
  private SQLConnection connection;

  public FirstTimeGameUpdater(Game game, SQLConnection connection) {
    this.game = game;
    this.connection = connection;
  }

  public void updateGame() {
    updateSteamGameAttributes();
  }

  private void updateSteamGameAttributes() {

  }

}
