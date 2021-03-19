package com.mayhew3.mediamogul.games;

import callback.OnSuccessCallback;
import com.mayhew3.mediamogul.EnvironmentChecker;
import com.mayhew3.postgresobject.exception.MissingEnvException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.json.JSONArray;
import wrapper.IGDBWrapper;
import wrapper.Parameters;
import wrapper.Version;

public class IGDBUpdateTestRunner {

  private static Logger logger = LogManager.getLogger(IGDBUpdateTestRunner.class);
  /*

    Test utility to see if threads close in most basic case. They don't.

  * */
  public static void main(String[] args) throws MissingEnvException {
    IGDBWrapper igdbWrapper = new IGDBWrapper(EnvironmentChecker.getOrThrow("igdb_key"), Version.STANDARD, false);

    Parameters parameters = new Parameters()
        .addSearch("Forza Horizon 4")
        .addFields("name,cover")
        .addLimit("5")
        .addOffset("0");

    igdbWrapper.games(parameters, new OnSuccessCallback() {
      @Override
      public void onSuccess(@NotNull JSONArray jsonArray) {
        debug(jsonArray);
      }

      @Override
      public void onError(@NotNull Exception e) {
        throw new RuntimeException(e);
      }
    });



  }

  private static void debug(Object message) {
    logger.debug(message);
  }



}
