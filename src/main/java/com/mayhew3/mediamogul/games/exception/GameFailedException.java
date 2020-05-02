package com.mayhew3.mediamogul.games.exception;

import com.mayhew3.mediamogul.exception.SingleFailedException;

public class GameFailedException extends SingleFailedException {
  public GameFailedException(String errorMessage) {
    super(errorMessage);
  }
}
