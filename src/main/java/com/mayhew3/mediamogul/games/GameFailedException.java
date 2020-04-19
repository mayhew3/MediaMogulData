package com.mayhew3.mediamogul.games;

public class GameFailedException extends SingleFailedException {
  public GameFailedException(String errorMessage) {
    super(errorMessage);
  }
}
