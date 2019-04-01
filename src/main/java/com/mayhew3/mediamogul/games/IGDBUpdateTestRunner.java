package com.mayhew3.mediamogul.games;

import callback.OnSuccessCallback;
import com.mayhew3.mediamogul.EnvironmentChecker;
import com.mayhew3.mediamogul.exception.MissingEnvException;
import org.jetbrains.annotations.NotNull;
import org.json.JSONArray;
import wrapper.IGDBWrapper;
import wrapper.Parameters;
import wrapper.Version;

public class IGDBUpdateTestRunner {


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
        System.out.println(jsonArray);
      }

      @Override
      public void onError(@NotNull Exception e) {
        throw new RuntimeException(e);
      }
    });



  }

  protected static void debug(Object object) {
    System.out.println(object);
  }



}
