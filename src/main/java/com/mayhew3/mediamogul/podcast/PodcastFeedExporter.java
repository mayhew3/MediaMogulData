package com.mayhew3.mediamogul.podcast;

import com.mayhew3.mediamogul.tv.blog.TemplatePrinter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.web.util.UriUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class PodcastFeedExporter {

  private TemplatePrinter innerTemplate;
  private TemplatePrinter outerTemplate;

  private String audioDirectory;

  private String outputPath;
  private String templatePath;

  Logger logger = LogManager.getLogger(PodcastFeedExporter.class);

  private PodcastFeedExporter(String templatePath, String outputPath, String audioDirectory) throws IOException {
    this.innerTemplate = createTemplate(templatePath + "/episode.xml");
    this.outerTemplate = createTemplate(templatePath + "/rss_outside.xml");

    this.templatePath = templatePath;
    this.outputPath = outputPath;
    this.audioDirectory = audioDirectory;
  }

  public static void main(String... args) throws IOException, ParseException {
    PodcastFeedExporter podcastFeedExporter = new PodcastFeedExporter(
        "D:\\Code\\git\\media-mogul-data\\resources\\RSSTemplates",
        "D:\\Code\\1and1\\dungeons",
        "D:\\OneDrive\\Games\\Roleplaying\\D&D\\2019 - Hunter Campaign\\Audio Files");
    podcastFeedExporter.execute();
  }

  private void execute() throws IOException, ParseException {
    File outputFile = new File(outputPath + "/podcast.rss.xml");
    FileOutputStream outputStream = new FileOutputStream(outputFile, false);

    StringBuilder stringBuilder = new StringBuilder();

    String innerExport = getAllEpisodes();

    outerTemplate.clearMappings();
    outerTemplate.addMapping("ITEMS", innerExport);

    String outerExport = outerTemplate.createCombinedExport();

    stringBuilder.append(outerExport);

    String fullOutput = stringBuilder.toString();
    outputStream.write(fullOutput.getBytes());
    outputStream.close();
  }

  private String getAllEpisodes() throws ParseException {
    JSONArray jsonArray = parseJSONObject(templatePath + "/metainfo.json");

    logger.info("Looking at audio files...");

    StringBuilder stringBuilder = new StringBuilder();

    for (Object obj : jsonArray) {
      JSONObject arc = (JSONObject) obj;
      String arcInfo = getArcInfo(arc);
      stringBuilder.append(arcInfo);
    }

    return stringBuilder.toString();
  }

  private String getArcInfo(JSONObject arc) throws ParseException {
    String title = arc.getString("title");
    String summary = arc.getString("summary");

    StringBuilder stringBuilder = new StringBuilder();

    JSONArray sessions = arc.getJSONArray("sessions");
    int sessionFileNumber = 1;
    for (Object obj : sessions) {
      JSONObject session = (JSONObject) obj;
      String date = session.getString("date");

      SimpleDateFormat originalDateFormat = new SimpleDateFormat("yyyy/MM/dd");
      Date realDate = originalDateFormat.parse(date);

      JSONArray files = session.getJSONArray("files");
      int fileNum = 1;
      for (Object obj2 : files) {
        JSONObject file = (JSONObject) obj2;
        String filename = file.getString("filename");
        String duration = file.getString("duration");

        String filePath = audioDirectory + "/" + filename + ".m4a";
        File audioFile = new File(filePath);
        long lengthInBytes = audioFile.length();

        String fileNameFormatted = UriUtils.encodePath(filename, "UTF-8");

        DateTime dateWithTime = new DateTime(realDate).plusHours(17 + 2 * fileNum);

        SimpleDateFormat dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss Z");
        dateFormat.setTimeZone(TimeZone.getTimeZone("PST"));
        String formattedDate = dateFormat.format(dateWithTime.toDate());

        String fileTitle = title + " (Part " + sessionFileNumber + ")";

        innerTemplate.clearMappings();
        innerTemplate.addMapping("TITLE", fileTitle);
        innerTemplate.addMapping("FILENAME", fileNameFormatted);
        innerTemplate.addMapping("GUID", fileNameFormatted);
        innerTemplate.addMapping("SUMMARY", summary);
        innerTemplate.addMapping("DESCRIPTION", summary);
        innerTemplate.addMapping("PUBDATE", formattedDate);
        innerTemplate.addMapping("DURATION", duration);
        innerTemplate.addMapping("LENGTH", String.valueOf(lengthInBytes));

        stringBuilder.append(innerTemplate.createCombinedExport());

        fileNum++;
        sessionFileNumber++;
      }
    }

    return stringBuilder.toString();
  }

  private TemplatePrinter createTemplate(String fileName) throws IOException {
    Path templateFile = Paths.get(fileName);
    String template = new String(Files.readAllBytes(templateFile));
    return new TemplatePrinter(template);
  }

  private JSONArray parseJSONObject(String filepath) {
    try {
      byte[] bytes = Files.readAllBytes(Paths.get(filepath));
      String text = new String(bytes, Charset.defaultCharset());
      return new JSONArray(text);
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException("Unable to read from file path: " + filepath);
    }
  }

}
