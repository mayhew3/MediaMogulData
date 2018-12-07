package com.mayhew3.mediamogul.games;

import com.google.common.collect.Lists;
import com.mayhew3.postgresobject.db.SQLConnection;
import com.mayhew3.mediamogul.model.games.Game;
import com.mayhew3.mediamogul.model.games.SteamAttribute;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.ui.Select;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;

public class SteamAttributeUpdater {

  private Game game;
  private SQLConnection connection;
  private WebDriver driver;

  public SteamAttributeUpdater(Game game, SQLConnection connection, WebDriver webDriver) {
    this.game = game;
    this.connection = connection;
    this.driver = webDriver;
  }

  public void runUpdater() throws GameFailedException, SQLException {
    parseSteamPage();
  }

  private void parseSteamPage() throws GameFailedException, SQLException {
    Integer steamID = game.steamID.getValue();

    String url = "http://store.steampowered.com/app/" + steamID + "/";

    driver.get(url);

    WebElement categoryBlock = getCategoryBlock(driver);

    if (categoryBlock == null) {
      game.steam_page_gone.changeValue(new Timestamp(new Date().getTime()));
      game.commit(connection);
      throw new GameFailedException("Resulting page had neither category_block nor agecheck_form. Url: " + url);
    }

    List<String> attributes = Lists.newArrayList();

    for (WebElement element : categoryBlock.findElements(By.className("name"))) {
      String attribute = element.getText();
      attributes.add(attribute);
    }

    game.steam_cloud.changeValue(false);
    game.steam_controller.changeValue(false);
    game.steam_local_coop.changeValue(false);

    for (String attribute : attributes) {
      SteamAttribute steamAttribute = new SteamAttribute();
      steamAttribute.initializeForInsert();

      steamAttribute.steamID.changeValue(steamID);
      steamAttribute.attribute.changeValue(attribute);
      steamAttribute.gameID.changeValue(game.id.getValue());
      steamAttribute.commit(connection);

      if (attribute.equalsIgnoreCase("Steam Cloud")) {
        game.steam_cloud.changeValue(true);
      }
      if (attribute.equalsIgnoreCase("Local Co-op")) {
        game.steam_local_coop.changeValue(true);
      }
      if (attribute.equalsIgnoreCase("Full controller support") ||
          attribute.equalsIgnoreCase("Partial Controller Support")) {
        game.steam_controller.changeValue(true);
      }
    }

    game.steam_attributes.changeValue(new Timestamp(new Date().getTime()));
    game.steam_attribute_count.changeValue(attributes.size());

    game.commit(connection);
  }


  private static WebElement getCategoryBlock(WebDriver driver) {

    // Find the text input element by its name
    List<WebElement> elements = driver.findElements(By.id("category_block"));

    if (!elements.isEmpty()) {
      return elements.get(0);
    }

    List<WebElement> forms = driver.findElements(By.id("agecheck_form"));
    if (forms.isEmpty()) {
      return null;
    }
    WebElement form = forms.get(0);

    Select daySelect = new Select(form.findElement(By.name("ageDay")));
    Select monthSelect = new Select(form.findElement(By.name("ageMonth")));
    Select yearSelect = new Select(form.findElement(By.id("ageYear")));
    WebElement enter = form.findElement(By.linkText("Enter"));

    daySelect.selectByValue("1");
    monthSelect.selectByValue("December");
    yearSelect.selectByValue("1980");

    enter.click();

    elements = driver.findElements(By.id("category_block"));
    if (elements.isEmpty()) {
      return null;
    } else {
      return elements.get(0);
    }
  }

}
