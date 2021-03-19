package com.mayhew3.mediamogul;

import com.mayhew3.postgresobject.EnvironmentChecker;
import com.mayhew3.postgresobject.exception.MissingEnvException;
import io.github.bonigarcia.wdm.WebDriverManager;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;

public class ChromeProvider {
  private ChromeDriver chromeDriver;

  public ChromeProvider() throws MissingEnvException {
    maybeSetDriverPath();
    WebDriverManager.chromedriver().setup();
  }

  public ChromeDriver openBrowser() {
    if (chromeDriver == null) {
      ChromeOptions chromeOptions = new ChromeOptions()
          .setHeadless(true);
      chromeDriver = new ChromeDriver(chromeOptions);
    }
    return chromeDriver;
  }

  public void closeBrowser() {
    if (chromeDriver != null) {
      chromeDriver.quit();
      chromeDriver = null;
    }
  }

  private static void maybeSetDriverPath() throws MissingEnvException {
    String envName = EnvironmentChecker.getOrThrow("envName");

    if (!"Heroku".equals(envName)) {
      String driverPath = System.getProperty("user.dir") + "\\resources\\chromedriver.exe";
      System.setProperty("webdriver.chrome.driver", driverPath);
    }
  }

}
