package com.mayhew3.mediamogul;

import com.mayhew3.mediamogul.games.exception.MetacriticElementNotFoundException;
import com.mayhew3.mediamogul.games.exception.MetacriticPageNotFoundException;
import com.mayhew3.postgresobject.db.SQLConnection;
import org.jetbrains.annotations.NotNull;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.select.Elements;

import java.io.IOException;

public abstract class MetacriticUpdater {

  protected final SQLConnection connection;

  public MetacriticUpdater(SQLConnection connection) {
    this.connection = connection;
  }

  @NotNull
  protected String formatTitle(String title, String hint) {
    return hint == null ?
        title
        .toLowerCase()
        .replaceAll(" - ", "-")
        .replaceAll(" ", "-")
        .replaceAll("'", "")
        .replaceAll("\\.", "")
        .replaceAll("\u2122", "") // (tm)
        .replaceAll("\u00AE", "") // (r)
        .replaceAll(":", "")
        .replaceAll("&", "and")
        .replaceAll(",", "")
        .replaceAll("\\(", "")
        .replaceAll("\\)", "")
        :
        hint;
  }

  protected Document getDocument(String prefix, String title) throws MetacriticPageNotFoundException {
    try {
      return Jsoup.connect("http://www.metacritic.com/" + prefix)
          .timeout(10000)
          .userAgent("Mozilla")
          .get();
    } catch (IOException e) {
      throw new MetacriticPageNotFoundException("Couldn't find Metacritic page for " + prefix + " '" + title + "' with formatted '" + prefix + "'");
    }
  }

  protected int getMetacriticFromDocument(Document document) throws MetacriticElementNotFoundException {
    Elements elements = document.select("script[type*=application/ld+json]");
    Element first1 = elements.first();

    if (first1 == null) {
      throw new MetacriticElementNotFoundException("Page found, but no element found with 'ratingValue' id.");
    }

    Node metaJSON = first1.childNodes().get(0);

    try {
      JSONObject jsonObject = new JSONObject(metaJSON.toString());
      JSONObject aggregateRating = jsonObject.getJSONObject("aggregateRating");
      String ratingValue = aggregateRating.getString("ratingValue");

      return Integer.parseInt(ratingValue);
    } catch (Exception e) {
      throw new MetacriticElementNotFoundException("Error parsing Metacritic page: " + e.getLocalizedMessage());
    }
  }

}
