package com.mayhew3.mediamogul.games;

import com.mayhew3.postgresobject.db.SQLConnection;
import com.mayhew3.mediamogul.model.games.Game;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.openqa.selenium.*;
import org.openqa.selenium.interactions.Actions;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;

public class HowLongToBeatUpdater {

  private Game game;
  private SQLConnection connection;
  private WebDriver driver;
  final private Integer indexColumn = 0;
  final private Integer polledColumn = 1;
  final private Integer medianColumn = 3;

  HowLongToBeatUpdater(Game game, SQLConnection connection, WebDriver webDriver) {
    this.game = game;
    this.connection = connection;
    this.driver = webDriver;
  }

  public void runUpdater() throws GameFailedException, SQLException {
    String title = game.title.getValue();
    title = title.replace("™", "");

    Integer howlong_id = game.howlong_id.getValue();

    String gameUrl;
    if (howlong_id == null) {
      gameUrl = findGame(title);
    } else {
      gameUrl = "http://howlongtobeat.com/game.php?id=" + howlong_id;
      driver.get(gameUrl);
    }

    updateTitle();

    WebElement game_main_table = findCorrectTable();

    if (game_main_table == null) {
      throw new GameFailedException("Couldn't find game times table on detail page: " + gameUrl);
    }

    validateColumns(game_main_table);

    populateTimesFromTable(game_main_table);

    game.howlong_updated.changeValue(new Timestamp(new Date().getTime()));
    game.commit(connection);
  }

  private void updateTitle() throws GameFailedException {
    WebElement profile_header = driver.findElement(By.className("profile_header"));
    String howlong_title = profile_header.getText();
    if (howlong_title == null) {
      throw new GameFailedException("Unable to find title on page.");
    } else {
      game.howlong_title.changeValue(howlong_title);
    }
  }

  private WebElement findCorrectTable() {
    List<WebElement> allTables = driver.findElements(By.className("game_main_table"));
    for (WebElement table : allTables) {
      WebElement table_header = table.findElement(By.tagName("thead"));
      WebElement firstColumn = table_header.findElement(By.tagName("td"));
      if ("Single-Player".equalsIgnoreCase(firstColumn.getText())) {
        return table;
      }
    }
    return null;
  }

  @NotNull
  private String goToGameUrlFromProfile(String title) throws GameFailedException {
    String url = "http://howlongtobeat.com/user.php?n=mayhew3&s=games";
    driver.get(url);

    // use filters to select all games. tricky because there's not a good condition to look for to indicate it is finished loading.
    // could get triggered before all the filters are activated, and will throw from the data being stale. So wait 7 seconds.
    WebElement list_multi = driver.findElement(By.id("list_multi"));
    WebElement list_b = driver.findElement(By.id("list_b"));
    WebElement list_r = driver.findElement(By.id("list_r"));
    WebElement list_c = driver.findElement(By.id("list_c"));
    WebElement list_cp = driver.findElement(By.id("list_cp"));

    Actions actions = new Actions(driver);
    actions.moveToElement(list_multi).click()
        .moveToElement(list_b).click()
        .moveToElement(list_r).click()
        .moveToElement(list_c).click()
        .moveToElement(list_cp).click()
        .build()
        .perform();

    waitForSeconds(7);

    WebElement user_games = driver.findElement(By.id("user_games"));
    List<WebElement> links = user_games.findElements(By.tagName("a"));

    try {
      for (WebElement link : links) {
        if (title.equalsIgnoreCase(link.getText())) {
          link.click();
          return driver.getCurrentUrl();
        }
      }
    } catch (StaleElementReferenceException e) {
      e.printStackTrace();
      throw new GameFailedException("Stale element exception while getting list of links.");
    }

    throw new GameFailedException("No game found on profile page.");
  }


  private void waitForSeconds(Integer seconds) {
    // probably better way to do this.
    long end = System.currentTimeMillis() + (seconds*1000);
    while (System.currentTimeMillis() < end) {
      // do nothing;
    }
  }

  @NotNull
  private String findGame(String title) throws GameFailedException, SQLException {
    String currentUrl = goToGameUrlFromSearch(title);

    if (currentUrl == null) {
      currentUrl = goToGameUrlFromProfile(title);
    }

    String[] split = currentUrl.split("=");
    if (split.length != 2) {
      throw new GameFailedException("Unexpected URL format for game detail: " + currentUrl);
    }

    String gameID = split[1];
    game.howlong_id.changeValue(Integer.valueOf(gameID));
    game.commit(connection);

    return currentUrl;
  }

  @Nullable
  private String goToGameUrlFromSearch(String title) throws GameFailedException {
    String url = "http://howlongtobeat.com/user.php?n=mayhew3";

    driver.get(url);

    WebElement searchBox = getSearchBox();

    WebElement resultElement;
    resultElement = getResult(searchBox);

    if (resultElement == null) {
      debug("Unable to find exact match for game '" + title + "'");
      return null;
    }

    resultElement.click();

    return driver.getCurrentUrl();
  }

  private WebElement getSearchBox() throws GameFailedException {
    try {
      WebElement searchBox = goToSearchBox();
      if (searchBox == null) {
        throw new GameFailedException("Unable to find search box");
      }
      return searchBox;
    } catch (NoSuchElementException e) {
      throw new GameFailedException("Unable to find search box.");
    }
  }

  private WebElement getResult(WebElement searchBox) {
    String title = game.title.getValue();

    title = title.replace("™", "");

    Actions actions = new Actions(driver);
    actions.moveToElement(searchBox).click().sendKeys(title).build().perform();

    return findResultElement(By.xpath("//*[@title=\"" + title + "\"]"));
  }

  private WebElement goToSearchBox() throws NoSuchElementException {
    WebElement searchButton = driver.findElement(By.id("global_search_box"));
    Actions actions = new Actions(driver);
    actions.moveToElement(searchButton, 100, 0).click().build().perform();

    return findResultElement(By.id("global_search_box"));
  }

  private void populateTimesFromTable(WebElement game_main_table) throws GameFailedException {
    for (WebElement row : game_main_table.findElements(By.tagName("tr"))) {
      List<WebElement> columns = row.findElements(By.tagName("td"));
      String playStyle = columns.get(indexColumn).getText();
      if ("Main Story".equalsIgnoreCase(playStyle)) {
        TimeAndConfidence timeAndConfidence = parseRowIntoTimeAndConfidence(row);
        game.howlong_main.changeValue(timeAndConfidence.time);
        game.howlong_main_confidence.changeValue(timeAndConfidence.confidence);
      }
      if ("Main + Extras".equalsIgnoreCase(playStyle)) {
        TimeAndConfidence timeAndConfidence = parseRowIntoTimeAndConfidence(row);
        game.howlong_extras.changeValue(timeAndConfidence.time);
        game.howlong_extras_confidence.changeValue(timeAndConfidence.confidence);
      }
      if ("Completionists".equalsIgnoreCase(playStyle)) {
        TimeAndConfidence timeAndConfidence = parseRowIntoTimeAndConfidence(row);
        game.howlong_completionist.changeValue(timeAndConfidence.time);
        game.howlong_completionist_confidence.changeValue(timeAndConfidence.confidence);
      }
      if ("All PlayStyles".equalsIgnoreCase(playStyle)) {
        TimeAndConfidence timeAndConfidence = parseRowIntoTimeAndConfidence(row);
        game.howlong_all.changeValue(timeAndConfidence.time);
        game.howlong_all_confidence.changeValue(timeAndConfidence.confidence);
      }
    }
  }

  private void validateColumns(WebElement game_main_table) throws GameFailedException {
    WebElement table_header = game_main_table.findElement(By.tagName("thead"));
    List<WebElement> header_columns = table_header.findElements(By.tagName("td"));
    if (!"Single-Player".equalsIgnoreCase(header_columns.get(indexColumn).getText())) {
      throw new GameFailedException("Expected first column to have 'Single-Player' header.");
    }
    if (!"Polled".equalsIgnoreCase(header_columns.get(polledColumn).getText())) {
      throw new GameFailedException("Expected second column to have 'Polled' header.");
    }
    if (!"Median".equalsIgnoreCase(header_columns.get(medianColumn).getText())) {
      throw new GameFailedException("Expected second column to have 'Polled' header.");
    }
  }

  private TimeAndConfidence parseRowIntoTimeAndConfidence(WebElement row) {
    List<WebElement> columns = row.findElements(By.tagName("td"));

    String confidenceClass = columns.get(polledColumn).getAttribute("class");
    String medianString = columns.get(medianColumn).getText();

    Integer confidence = Integer.valueOf(confidenceClass.split("_")[1]);
    BigDecimal totalTime = getTotalTime(medianString);

    return new TimeAndConfidence(totalTime, confidence);
  }

  private BigDecimal getTotalTime(String medianString) {
    String[] split = medianString.split(" ");
    String firstPart = split[0];

    if (firstPart.contains("h")) {
      BigDecimal totalTime = convertHour(firstPart);

      if (split.length > 1) {
        BigDecimal hourFraction = convertMinutes(split[1]);
        totalTime = totalTime.add(hourFraction);
      }

      return totalTime;
    } else {
      return convertMinutes(firstPart);
    }
  }

  private BigDecimal convertHour(String firstPart) {
    String hourString = firstPart.replace("h", "");
    Integer hour = Integer.valueOf(hourString);
    return BigDecimal.valueOf(hour);
  }

  private BigDecimal convertMinutes(String s) {
    String minuteString = s.replace("m", "");
    Integer minute = Integer.valueOf(minuteString);
    BigDecimal minutePart = BigDecimal.valueOf(minute);
    return minutePart.divide(BigDecimal.valueOf(60), 4, BigDecimal.ROUND_HALF_EVEN);
  }

  @Nullable
  private WebElement findResultElement(By by) {
    long end = System.currentTimeMillis() + 5000;
    while (System.currentTimeMillis() < end) {
      List<WebElement> results = driver.findElements(by);

      // If results have been returned, the results are displayed in a drop down.
      if (!results.isEmpty()) {
        WebElement result = results.get(0);
        if (result.isDisplayed()) {
          return result;
        }
      }
    }
    return null;
  }

  protected void debug(Object object) {
    System.out.println(object);
  }


  private class TimeAndConfidence {
    public BigDecimal time;
    public Integer confidence;

    public TimeAndConfidence(BigDecimal time, Integer confidence) {
      this.time = time;
      this.confidence = confidence;
    }
  }


}
