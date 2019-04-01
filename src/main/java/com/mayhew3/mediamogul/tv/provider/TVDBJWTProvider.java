package com.mayhew3.mediamogul.tv.provider;

import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.http.auth.AuthenticationException;
import org.json.JSONObject;

import java.sql.Timestamp;

public interface TVDBJWTProvider {

  JSONObject findSeriesMatches(String formattedTitle) throws UnirestException, AuthenticationException;

  JSONObject getSeriesData(Integer tvdbSeriesId) throws UnirestException, AuthenticationException;

  JSONObject getEpisodeSummaries(Integer tvdbSeriesId, Integer pageNumber) throws UnirestException, AuthenticationException;

  JSONObject getEpisodeData(Integer tvdbEpisodeId) throws UnirestException, AuthenticationException;

  JSONObject getPosterData(Integer tvdbId) throws UnirestException, AuthenticationException;

  JSONObject getUpdatedSeries(Timestamp fromDate) throws UnirestException, AuthenticationException;

  public class EpisodeDetail {
    public Integer seriesNumber;
    public Integer episodeNumber;

    public EpisodeDetail(Integer seriesNumber, Integer episodeNumber) {
      this.seriesNumber = seriesNumber;
      this.episodeNumber = episodeNumber;
    }
  }

}
