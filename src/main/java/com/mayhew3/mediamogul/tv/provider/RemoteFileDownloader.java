package com.mayhew3.mediamogul.tv.provider;

import com.mayhew3.mediamogul.EnvironmentChecker;
import com.mayhew3.mediamogul.exception.MissingEnvException;
import org.jetbrains.annotations.Nullable;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.net.URL;
import java.net.URLConnection;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.text.SimpleDateFormat;
import java.util.Date;

public class RemoteFileDownloader implements TiVoDataProvider {
  private Boolean saveXml = false;
  private String localFolderPath;

  public RemoteFileDownloader(Boolean saveXml) throws MissingEnvException {
    String tivoApiKey = EnvironmentChecker.getOrThrow("TIVO_API_KEY");

    Authenticator.setDefault (new Authenticator() {
      protected PasswordAuthentication getPasswordAuthentication() {
        return new PasswordAuthentication("tivo", tivoApiKey.toCharArray());
      }
    });

    this.saveXml = saveXml;
    if (this.saveXml) {
      SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy_MM_dd_hh_mm_ss");
      String dateFormatted = simpleDateFormat.format(new Date());

      localFolderPath = "src\\test\\resources\\tivo_xml\\" + dateFormatted + "\\";
      if (!new File(localFolderPath).mkdir()) {
        throw new RuntimeException("Unable to create directory: " + localFolderPath);
      }
    }
  }

  public Document connectAndRetrieveDocument(String urlString, @Nullable String episodeIdentifier) throws IOException, SAXException {
    URL url = new URL(urlString);

    URLConnection urlConnection = url.openConnection();
    urlConnection.setConnectTimeout(15000);
    urlConnection.setReadTimeout(30000);

    try (InputStream is = urlConnection.getInputStream()) {

      if (this.saveXml) {
        String fileName = episodeIdentifier == null ? "00_index" : episodeIdentifier;

        File destination = new File(localFolderPath + fileName + ".xml");
        FileOutputStream fos = new FileOutputStream(destination, true);

        ReadableByteChannel readableByteChannel = Channels.newChannel(is);
        FileChannel destChannel = fos.getChannel();

        destChannel.transferFrom(readableByteChannel, 0, Long.MAX_VALUE);
        fos.close();

        FileInputStream fileInputStream = new FileInputStream(destination);
        return recoverDocument(fileInputStream);
      }

      return recoverDocument(is);
    }
  }

  private Document recoverDocument(InputStream inputStream) throws IOException, SAXException {
    DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
    DocumentBuilder dBuilder = null;
    try {
      dBuilder = dbFactory.newDocumentBuilder();
    } catch (ParserConfigurationException e) {
      e.printStackTrace();
    }

    Document doc;
    assert dBuilder != null;
    doc = dBuilder.parse(inputStream);
    return doc;
  }



}
