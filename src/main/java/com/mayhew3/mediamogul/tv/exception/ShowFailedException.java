package com.mayhew3.mediamogul.tv.exception;

import com.mayhew3.mediamogul.exception.SingleFailedException;

public class ShowFailedException extends SingleFailedException {
  public ShowFailedException(String errorMessage) {
    super(errorMessage);
  }
}
