package com.mayhew3.mediamogul.tv;

import info.debatty.java.stringsimilarity.NGram;
import info.debatty.java.stringsimilarity.interfaces.StringDistance;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

public class StringSimilarityTest {

  private static Logger logger = LogManager.getLogger(StringSimilarityTest.class);
  
  private void debug(Object message) {
    logger.debug(message);
  }
  
  @Test
  public void testNGram() {
    StringDistance levenshtein = new NGram();

    debug(levenshtein.distance("IX", "IX"));
    debug(levenshtein.distance("IX", "IX."));
    debug(levenshtein.distance("IX", "XX"));
    debug(levenshtein.distance("IX", "XX."));
    debug(levenshtein.distance("Star Wars", "Star Wars: The Next Generation"));
    debug(levenshtein.distance("Star Wars", "Flippy Fuhrmat and the Peace Patrol"));
    debug(levenshtein.distance("Star Wars The Next Generation", "Star Wars: The Next Generation"));
  }
}
