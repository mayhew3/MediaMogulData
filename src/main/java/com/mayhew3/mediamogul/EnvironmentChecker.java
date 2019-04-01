package com.mayhew3.mediamogul;

import com.mayhew3.mediamogul.exception.MissingEnvException;

public class EnvironmentChecker {
  public static String getOrThrow(String envLabel) throws MissingEnvException {
    String envValue = System.getenv(envLabel);
    if (envValue == null) {
      throw new MissingEnvException("No environment variable found for '" + envLabel + "'!");
    }

    return envValue;
  }
}
