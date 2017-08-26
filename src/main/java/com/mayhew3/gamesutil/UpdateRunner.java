package com.mayhew3.gamesutil;

import com.mashape.unirest.http.exceptions.UnirestException;
import com.mayhew3.gamesutil.xml.BadlyFormattedXMLException;

import java.sql.SQLException;

public interface UpdateRunner {
  String getRunnerName();

  void runUpdate() throws SQLException, BadlyFormattedXMLException;
}
