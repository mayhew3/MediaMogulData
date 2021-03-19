package com.mayhew3.mediamogul.tv;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.mayhew3.mediamogul.SSLTool;
import com.mayhew3.mediamogul.model.tv.*;
import com.mayhew3.mediamogul.scheduler.UpdateRunner;
import com.mayhew3.mediamogul.tv.exception.ShowFailedException;
import com.mayhew3.mediamogul.tv.helper.UpdateMode;
import com.mayhew3.mediamogul.tv.provider.RemoteFileDownloader;
import com.mayhew3.mediamogul.tv.provider.TiVoDataProvider;
import com.mayhew3.mediamogul.xml.BadlyFormattedXMLException;
import com.mayhew3.mediamogul.xml.NodeReader;
import com.mayhew3.mediamogul.xml.NodeReaderImpl;
import com.mayhew3.postgresobject.ArgumentChecker;
import com.mayhew3.postgresobject.db.PostgresConnectionFactory;
import com.mayhew3.postgresobject.db.SQLConnection;
import com.mayhew3.postgresobject.exception.MissingEnvException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

public class TiVoCommunicator implements UpdateRunner {

  private List<String> episodesOnTiVo;
  private List<String> moviesOnTiVo;
  private NodeReader nodeReader;

  private Integer addedShows = 0;
  private Integer deletedShows = 0;
  private Integer updatedShows = 0;

  private UpdateMode updateMode;

  private static SQLConnection sqlConnection;
  private TiVoDataProvider tiVoDataProvider;

  private static Logger logger = LogManager.getLogger(TiVoCommunicator.class);

  public TiVoCommunicator(SQLConnection connection, TiVoDataProvider tiVoDataProvider, UpdateMode updateMode) {
    episodesOnTiVo = new ArrayList<>();
    moviesOnTiVo = new ArrayList<>();
    nodeReader = new NodeReaderImpl();

    sqlConnection = connection;
    this.tiVoDataProvider = tiVoDataProvider;

    HashSet<UpdateMode> validModes = Sets.newHashSet(UpdateMode.FULL, UpdateMode.QUICK);

    if (!validModes.contains(updateMode)) {
      throw new IllegalArgumentException("Update mode '" + updateMode + "' is not applicable for this updater.");
    }

    this.updateMode = updateMode;
  }

  public static void main(String[] args) throws SQLException, URISyntaxException, BadlyFormattedXMLException, MissingEnvException {
    List<String> argList = Lists.newArrayList(args);
    Boolean saveTiVoXML = argList.contains("SaveTiVoXML");

    ArgumentChecker argumentChecker = new ArgumentChecker(args);
    UpdateMode updateMode = UpdateMode.getUpdateModeOrDefault(argumentChecker, UpdateMode.QUICK);

    SQLConnection connection = PostgresConnectionFactory.createConnection(argumentChecker);

    TiVoCommunicator tiVoCommunicator = new TiVoCommunicator(connection, new RemoteFileDownloader(saveTiVoXML), updateMode);

    tiVoCommunicator.runUpdate();

    new SeriesDenormUpdater(connection).runUpdate();
  }

  public void runUpdate() throws SQLException, BadlyFormattedXMLException {

    SSLTool.disableCertificateValidation();

    /*
      What I learned:
      - SSL is a pain in the butt.
      - There may be a magic combination of saving the certificate from the browser, running the keytool utility,
        adding the right VM parameters to recognize it, create a Keystore, Trusting something, running it through
        an SSL Factory and Https connection, but I was never able to get it to recognize the certificate.
      - Because I'm only running this over the local network, I'm just disabling the validation completely, which
        is hacky, but seems to work. (After getting the Authorization popup to work. See RemoteFileDownloader()
     */

    updateFields();

  }

  private void updateFields() throws SQLException, BadlyFormattedXMLException {
    String fullURL = "https://10.0.0.14/TiVoConnect?Command=QueryContainer&Container=%2FNowPlaying&Recurse=Yes&ItemCount=50";

    try {
      Boolean keepGoing = true;
      Integer offset = 0;

      while (keepGoing) {
        debug("Downloading entries " + offset + " to " + (offset + 50) + "...");

        Document document = readXMLFromTivoUrl(fullURL + "&AnchorOffset=" + offset, null);

        debug("Checking against DB...");
        keepGoing = parseShowsFromDocument(document);
        offset += 50;
      }

      if (isFullUpdate()) {
        checkForDeletedShows();
        checkForDeletedMovies();
        // todo: delete TiVo suggestions from DB completely if they're deleted. Don't need that noise.
      }

      debug("Finished.");

      debug("Added: " + addedShows);
      debug("Updated: " + updatedShows);
      debug("Deleted: " + deletedShows);

    } catch (SAXException | IOException e) {
      debug("Error reading from URL: " + fullURL);
      e.printStackTrace();
    }
  }

  private void checkForDeletedShows() throws SQLException {
    ResultSet resultSet = sqlConnection.executeQuery(
        "SELECT * " +
            "FROM tivo_episode " +
            "WHERE deleted_date IS NULL"
    );

    while (resultSet.next()) {
      TiVoEpisode tiVoEpisode = new TiVoEpisode();
      tiVoEpisode.initializeFromDBObject(resultSet);

      deleteIfGone(tiVoEpisode);
    }

  }

  private void checkForDeletedMovies() throws SQLException {
    ResultSet resultSet = sqlConnection.executeQuery(
        "SELECT * " +
            "FROM movie " +
            "WHERE deleted_date IS NULL"
    );

    while (resultSet.next()) {
      Movie movie = new Movie();
      movie.initializeFromDBObject(resultSet);

      deleteIfGone(movie);
    }

  }

  private void deleteIfGone(TiVoEpisode tiVoEpisode) throws SQLException {
    String programV2Id = tiVoEpisode.programV2Id.getValue();

    if (programV2Id == null || !episodesOnTiVo.contains(programV2Id)) {
      Date showingTime = tiVoEpisode.showingStartTime.getValue();

      if (showingTime == null) {
        debug("Found episode in DB that is no longer on Tivo: '" + tiVoEpisode.title.getValue() + "' on UNKNOWN DATE. Updating deletion date.");
      } else {
        String formattedDate = new SimpleDateFormat("yyyy-MM-dd").format(showingTime);
        debug("Found episode in DB that is no longer on Tivo: '" + tiVoEpisode.title.getValue() + "' on " + formattedDate + ". Updating deletion date.");
      }

      tiVoEpisode.deletedDate.changeValue(new Date());
      tiVoEpisode.recordingNow.changeValue(false);
      tiVoEpisode.commit(sqlConnection);

      deletedShows++;
    }
  }

  private void deleteIfGone(Movie movie) throws SQLException {
    String programId = movie.programV2Id.getValue();

    if (programId == null || !moviesOnTiVo.contains(programId)) {
      Date showingTime = movie.showingStartTime.getValue();

      if (showingTime == null) {
        debug("Found movie in DB that is no longer on Tivo: '" + movie.title.getValue() + "' on UNKNOWN DATE. Updating deletion date.");
      } else {
        String formattedDate = new SimpleDateFormat("yyyy-MM-dd").format(showingTime);
        debug("Found movie in DB that is no longer on Tivo: '" + movie.title.getValue() + "' on " + formattedDate + ". Updating deletion date.");
      }

      movie.deletedDate.changeValue(new Date());
      movie.commit(sqlConnection);

      deletedShows++;
    }
  }

  private boolean parseShowsFromDocument(Document document) throws SQLException, BadlyFormattedXMLException {
    NodeList nodeList = document.getChildNodes();

    Node tivoContainer = nodeReader.getNodeWithTag(nodeList, "TiVoContainer");
    NodeList childNodes = tivoContainer.getChildNodes();

    Boolean shouldBeLastDocument = false;

    Integer showNumber = 0;
    for (int i = 0; i < childNodes.getLength(); i++) {
      Node item = childNodes.item(i);
      if (item.getNodeName().equals("Item")) {
        NodeList showAttributes = item.getChildNodes();
        Boolean alreadyExists = false;

        try {
          alreadyExists = parseAndUpdateSingleShow(showAttributes);
        } catch (Exception e) {
          e.printStackTrace();
          debug("Failed to parse TVDB data.");
        }

        if (alreadyExists) {
          shouldBeLastDocument = true;
        }
        showNumber++;
      }
    }

    return !shouldBeLastDocument && showNumber == 50;
  }


  /*
   * @param showAttributes Root XML object
   * @return Whether this episode already exists in the database, and we should stop updating later episodes in quick mode.
   * @throws SQLException
   */
  private Boolean parseAndUpdateSingleShow(NodeList showAttributes) throws SQLException, BadlyFormattedXMLException, ShowFailedException {

    NodeList showDetails = nodeReader.getNodeWithTag(showAttributes, "Details").getChildNodes();

    TiVoInfo tivoInfo = new TiVoInfo(showDetails, nodeReader);

    if (isRecordingNow(showAttributes)) {
      tivoInfo.recordingNow = true;
    } else {
      tivoInfo.recordingNow = false;
      tivoInfo.isSuggestion = isSuggestion(showAttributes);
    }

    tivoInfo.url = getUrl(showAttributes);

    if (isEpisodic(showAttributes, tivoInfo.seriesTitle + "_" + tivoInfo.programId)) {

      Series series = getOrCreateSeries(tivoInfo);
      if (series != null) {

        if (isFullUpdate()) {
          episodesOnTiVo.add(tivoInfo.programId);
        }

        return addEpisodeIfNotExists(showDetails, series, tivoInfo);
      } else {
        return false;
      }
    } else {
      return addMovieIfNotExists(showDetails, tivoInfo);
    }
  }

  /*
   * @return New series object based on TiVo info, or existing series object in DB, or null
   *        if we chose not to create a new series, because the first episode is recording now,
   *        and we don't know if it is a suggestion or not.
   * @throws SQLException
   */
  @Nullable
  private Series getOrCreateSeries(TiVoInfo tivoInfo) throws SQLException {
    Series existingSeries = findExistingSeries(tivoInfo);

    if (existingSeries == null) {
      if (!tivoInfo.recordingNow) {
        debug("Adding series '" + tivoInfo.seriesTitle + "'.");
        addNewSeries(tivoInfo);
      } else {
        debug("Found new series '" + tivoInfo.seriesTitle + "', but it is currently recording. Waiting to add it.");
      }
      return null;
    } else {
      debug("Updating existing series '" + tivoInfo.seriesTitle + "'.");
      return existingSeries;
    }
  }

  @Nullable
  private Series findExistingSeries(TiVoInfo tivoInfo) throws SQLException {
    Series series = new Series();

    ResultSet resultSet = sqlConnection.prepareAndExecuteStatementFetch(
        "SELECT * " +
            "FROM series " +
            "WHERE tivo_series_v2_ext_id = ? " +
            "and retired = ? ",
        tivoInfo.tivoId, 0);

    if (resultSet.next()) {
      series.initializeFromDBObject(resultSet);
      return series;
    } else {
      @NotNull ResultSet maybeMatch = sqlConnection.prepareAndExecuteStatementFetch(
          "SELECT * " +
              "FROM series " +
              "WHERE title = ? " +
              "AND (tivo_series_v2_ext_id is null OR tivo_version = ?) " +
              "AND retired = ? ",
          tivoInfo.seriesTitle, 1, 0);

      if (maybeMatch.next()) {
        series.initializeFromDBObject(maybeMatch);
        updateTiVoFieldsOnExistingSeries(tivoInfo, series);
        return series;
      }
    }

    return null;
  }

  private void updateTiVoFieldsOnExistingSeries(TiVoInfo tivoInfo, Series series) throws SQLException {
    series.tivoSeriesV2ExtId.changeValue(tivoInfo.tivoId);
    series.tivoName.changeValue(tivoInfo.seriesTitle);
    if (tivoInfo.isSuggestion == null || !tivoInfo.isSuggestion) {
      series.isSuggestion.changeValue(false);
    }
    series.matchedWrong.changeValue(false);
    series.tivoVersion.changeValue(2);

    series.commit(sqlConnection);

    series.addViewingLocation(sqlConnection, "TiVo");
  }

  /*
   * @return Whether the episode already exists in the database.
   * @throws SQLException
   */
  private Boolean addEpisodeIfNotExists(NodeList showDetails, Series series, TiVoInfo tivoInfo) throws SQLException {
    ResultSet existingTiVoEpisode = getExistingTiVoEpisodes(tivoInfo);
    Boolean tivoEpisodeExists = existingTiVoEpisode.next();

    if (!tivoEpisodeExists) {
      existingTiVoEpisode = getExistingV1Match(tivoInfo);
      tivoEpisodeExists = existingTiVoEpisode.next();
    }


    TiVoEpisode tivoEpisode = getOrCreateTiVoEpisode(showDetails, tivoInfo, existingTiVoEpisode, tivoEpisodeExists);

    List<Episode> linkedEpisodes = tivoEpisode.getEpisodes(sqlConnection);

    // if we already have any linked episodes, don't try to match again.
    if (linkedEpisodes.isEmpty()) {
      List<Episode> fromRepeats = getLinkedFromRepeats(tivoEpisode);
      if (fromRepeats.isEmpty()) {

        TVDBEpisodeMatcher matcher = new TVDBEpisodeMatcher(sqlConnection, tivoEpisode, series.id.getValue());
        Optional<TVDBEpisode> tvdbEpisodeOptional = matcher.matchAndLinkEpisode();

        // if we found a good match for tivo episode, link it
        if (tvdbEpisodeOptional.isPresent()) {
          TVDBEpisode tvdbEpisode = tvdbEpisodeOptional.get();
          Episode episode = tvdbEpisode.getEpisode(sqlConnection);
          updatedShows++;
          updateSeriesDenorms(tivoEpisode, episode, series);
          series.commit(sqlConnection);
        }
      } else {
        for (Episode episode : fromRepeats) {
          episode.addToTiVoEpisodes(sqlConnection, tivoEpisode);
          updateSeriesDenorms(tivoEpisode, episode, series);
        }
        series.commit(sqlConnection);
      }
    }

    return tivoEpisodeExists && isQuickUpdate();
  }

  private List<Episode> getLinkedFromRepeats(TiVoEpisode tiVoEpisode) throws SQLException {
    String programId = tiVoEpisode.programV2Id.getValue();
    ResultSet resultSet = getExistingTiVoEpisodes(programId);

    while (resultSet.next()) {
      TiVoEpisode otherEpisode = new TiVoEpisode();
      otherEpisode.initializeFromDBObject(resultSet);

      if (!Objects.equals(otherEpisode.id.getValue(), tiVoEpisode.id.getValue())) {
        return otherEpisode.getEpisodes(sqlConnection);
      }
    }

    return new ArrayList<>();
  }

  @NotNull
  private ResultSet getExistingV1Match(TiVoInfo tivoInfo) throws SQLException {
    long numberOfSeconds = Long.decode(tivoInfo.captureDate);
    Timestamp captureDateTimestamp = new Timestamp(numberOfSeconds * 1000);

    return sqlConnection.prepareAndExecuteStatementFetch(
        "SELECT * " +
        "FROM tivo_episode " +
        "WHERE series_title = ? " +
        "AND capture_date = ? " +
        "AND tivo_version = ? " +
        "AND retired = ? ",
        tivoInfo.seriesTitle,
        captureDateTimestamp,
        1, // tivo_version
        0); // retired
  }


  @NotNull
  private ResultSet getExistingV1MovieMatch(TiVoInfo tivoInfo) throws SQLException {
    long numberOfSeconds = Long.decode(tivoInfo.captureDate);
    Timestamp captureDateTimestamp = new Timestamp(numberOfSeconds * 1000);

    return sqlConnection.prepareAndExecuteStatementFetch(
        "SELECT * " +
        "FROM movie " +
        "WHERE title = ? " +
        "AND capture_date = ? " +
        "AND retired = ? ",
        tivoInfo.seriesTitle,
        captureDateTimestamp,
        0); // retired
  }


  /*
   * @return Whether the movie already exists in the database.
   * @throws SQLException
   */
  private Boolean addMovieIfNotExists(NodeList showDetails, TiVoInfo tivoInfo) throws SQLException {
    ResultSet resultSet = getExistingMovies(tivoInfo.programId);
    boolean movieExists = resultSet.next();

    if (!movieExists) {
      resultSet = getExistingV1MovieMatch(tivoInfo);
      movieExists = resultSet.next();
    }

    if (isFullUpdate()) {
      moviesOnTiVo.add(tivoInfo.programId);
    }

    if (movieExists && isQuickUpdate()) {
      return true;
    }

    maybeCreateMovie(showDetails, tivoInfo, resultSet, movieExists);

    return false;
  }

  private TiVoEpisode getOrCreateTiVoEpisode(NodeList showDetails, TiVoInfo tivoInfo, ResultSet existingEpisode, Boolean tivoEpisodeExists) throws SQLException {
    TiVoEpisode tivoEpisode = new TiVoEpisode();
    if (tivoEpisodeExists) {
      tivoEpisode.initializeFromDBObject(existingEpisode);
    } else {
      tivoEpisode.initializeForInsert();
      tivoEpisode.tvdbMatchStatus.changeValue(TVDBMatchStatus.MATCH_FIRST_PASS);
    }

    formatEpisodeObject(tivoEpisode, tivoInfo.url, tivoInfo.isSuggestion, showDetails);
    tivoEpisode.tivoSeriesV2ExtId.changeValue(tivoInfo.tivoId);
    tivoEpisode.seriesTitle.changeValue(tivoInfo.seriesTitle);
    tivoEpisode.recordingNow.changeValue(tivoInfo.recordingNow);

    tivoEpisode.commit(sqlConnection);
    return tivoEpisode;
  }

  private void maybeCreateMovie(NodeList showDetails, TiVoInfo tivoInfo, ResultSet existingMovie, Boolean movieExists) throws SQLException {
    Movie movie = new Movie();
    if (movieExists) {
      movie.initializeFromDBObject(existingMovie);
    } else {
      movie.initializeForInsert();
    }

    formatMovieObject(movie, tivoInfo.url, tivoInfo.isSuggestion, showDetails);
    movie.tivoSeriesV2ExtId.changeValue(tivoInfo.tivoId);
    movie.seriesTitle.changeValue(tivoInfo.seriesTitle);

    // todo: check for duplicate (program_v2_id, retired). Do some research. In these cases, are two episodes
    // todo: on TiVo with same program_v2_id, or is one deleted but not marked yet? Either way I think we should update
    // todo: instead of insert.
    movie.commit(sqlConnection);
  }

  private void addNewSeries(TiVoInfo tivoInfo) throws SQLException {
    Series series = new Series();
    series.initializeForInsert();

    debug("Adding series '" + tivoInfo.seriesTitle + "'  with TiVoID '" + tivoInfo.tivoId + "'");

    series.initializeDenorms();

    series.tivoSeriesV2ExtId.changeValue(tivoInfo.tivoId);
    series.seriesTitle.changeValue(tivoInfo.seriesTitle);
    series.tivoName.changeValue(tivoInfo.seriesTitle);
    series.isSuggestion.changeValue(tivoInfo.isSuggestion);
    series.matchedWrong.changeValue(false);
    series.tvdbNew.changeValue(true);
    series.metacriticNew.changeValue(true);
    series.tivoVersion.changeValue(2);
    series.addedBy.changeValue("TiVo");
    series.tvdbMatchStatus.changeValue(TVDBMatchStatus.MATCH_FIRST_PASS);

    series.commit(sqlConnection);

    series.addViewingLocation(sqlConnection, "TiVo");

    addedShows++;
  }


  private Boolean isBefore(Date newDate, Date trackingDate) {
    return trackingDate == null || trackingDate.after(newDate);
  }


  private Boolean isAfter(Date newDate, Date trackingDate) {
    return trackingDate == null || trackingDate.before(newDate);
  }


  private void updateSeriesDenorms(TiVoEpisode tiVoEpisode, Episode episode, Series series) {

    Boolean suggestion = tiVoEpisode.suggestion.getValue();
    Date showingStartTime = tiVoEpisode.showingStartTime.getValue();
    Boolean watched = episode.watched.getValue();

    if (Boolean.TRUE.equals(suggestion)) {
      series.suggestionEpisodes.increment(1);
    } else {
      series.activeEpisodes.increment(1);
      series.unwatchedEpisodes.increment(1);
    }

    series.matchedEpisodes.increment(1);
    series.tvdbOnlyEpisodes.increment(-1);
    if (!watched) {
      series.unwatchedUnrecorded.increment(-1);

      if (isBefore(showingStartTime, series.firstUnwatched.getValue())) {
        series.firstUnwatched.changeValue(showingStartTime);
      }

      if (isAfter(showingStartTime, series.lastUnwatched.getValue())) {
        series.lastUnwatched.changeValue(showingStartTime);
      }

    }

    if (isAfter(showingStartTime, series.mostRecent.getValue())) {
      series.mostRecent.changeValue(showingStartTime);
    }

  }

  private ResultSet getExistingTiVoEpisodes(String programId) throws SQLException {
    return sqlConnection.prepareAndExecuteStatementFetch("SELECT * FROM tivo_episode WHERE program_v2_id = ? AND retired = ?", programId, 0);
  }

  private ResultSet getExistingTiVoEpisodes(TiVoInfo tiVoInfo) throws SQLException {
    long numberOfSeconds = Long.decode(tiVoInfo.captureDate);
    Timestamp captureDateTimestamp = new Timestamp(numberOfSeconds * 1000);

    return sqlConnection.prepareAndExecuteStatementFetch(
        "SELECT * " +
            "FROM tivo_episode " +
            "WHERE program_v2_id = ? " +
            "AND capture_date = ? " +
            "AND retired = ?",
        tiVoInfo.programId, captureDateTimestamp, 0);
  }

  private ResultSet getExistingMovies(String programId) throws SQLException {
    return sqlConnection.prepareAndExecuteStatementFetch("SELECT * FROM movie WHERE program_v2_id = ? AND retired = ?", programId, 0);
  }

  private void formatEpisodeObject(TiVoEpisode tiVoEpisode, String url, Boolean isSuggestion, NodeList showDetails) {
    tiVoEpisode.captureDate.changeValueFromXMLString(nodeReader.getValueOfSimpleStringNullableNode(showDetails, "CaptureDate"));
    tiVoEpisode.showingStartTime.changeValueFromXMLString(nodeReader.getValueOfSimpleStringNullableNode(showDetails, "ShowingStartTime"));

    tiVoEpisode.description.changeValueFromString(nodeReader.getValueOfSimpleStringNullableNode(showDetails, "Description"));
    tiVoEpisode.title.changeValueFromString(nodeReader.getValueOfSimpleStringNullableNode(showDetails, "EpisodeTitle"));
    tiVoEpisode.episodeNumber.changeValueFromString(nodeReader.getValueOfSimpleStringNullableNode(showDetails, "EpisodeNumber"));
    tiVoEpisode.hd.changeValueFromString(nodeReader.getValueOfSimpleStringNullableNode(showDetails, "HighDefinition"));
    tiVoEpisode.programV2Id.changeValueFromString(nodeReader.getValueOfSimpleStringNullableNode(showDetails, "ProgramId"));
    tiVoEpisode.duration.changeValueFromString(nodeReader.getValueOfSimpleStringNullableNode(showDetails, "Duration"));
    tiVoEpisode.showingDuration.changeValueFromString(nodeReader.getValueOfSimpleStringNullableNode(showDetails, "ShowingDuration"));
    tiVoEpisode.channel.changeValueFromString(nodeReader.getValueOfSimpleStringNullableNode(showDetails, "SourceChannel"));
    tiVoEpisode.station.changeValueFromString(nodeReader.getValueOfSimpleStringNullableNode(showDetails, "SourceStation"));
    tiVoEpisode.rating.changeValueFromString(nodeReader.getValueOfSimpleStringNullableNode(showDetails, "TvRating"));
    tiVoEpisode.tivoVersion.changeValue(2);
    tiVoEpisode.retired.changeValue(0);

    tiVoEpisode.suggestion.changeValue(isSuggestion);
    tiVoEpisode.url.changeValue(url);
  }

  private void formatMovieObject(Movie movie, String url, Boolean isSuggestion, NodeList showDetails) {
    movie.captureDate.changeValueFromXMLString(nodeReader.getValueOfSimpleStringNullableNode(showDetails, "CaptureDate"));
    movie.showingStartTime.changeValueFromXMLString(nodeReader.getValueOfSimpleStringNullableNode(showDetails, "ShowingStartTime"));

    movie.description.changeValueFromString(nodeReader.getValueOfSimpleStringNullableNode(showDetails, "Description"));
    movie.title.changeValueFromString(nodeReader.getValueOfSimpleStringNullableNode(showDetails, "Title"));
    movie.hd.changeValueFromString(nodeReader.getValueOfSimpleStringNullableNode(showDetails, "HighDefinition"));
    movie.programV2Id.changeValueFromString(nodeReader.getValueOfSimpleStringNullableNode(showDetails, "ProgramId"));
    movie.duration.changeValueFromString(nodeReader.getValueOfSimpleStringNullableNode(showDetails, "Duration"));
    movie.showingDuration.changeValueFromString(nodeReader.getValueOfSimpleStringNullableNode(showDetails, "ShowingDuration"));
    movie.channel.changeValueFromString(nodeReader.getValueOfSimpleStringNullableNode(showDetails, "SourceChannel"));
    movie.station.changeValueFromString(nodeReader.getValueOfSimpleStringNullableNode(showDetails, "SourceStation"));
    movie.rating.changeValueFromString(nodeReader.getValueOfSimpleStringNullableNode(showDetails, "TvRating"));
    movie.retired.changeValue(0);

    movie.suggestion.changeValue(isSuggestion);
    movie.url.changeValue(url);
  }


  @NotNull
  private Boolean isSuggestion(NodeList showAttributes) throws BadlyFormattedXMLException {
    NodeList links = nodeReader.getNodeWithTag(showAttributes, "Links").getChildNodes();
    Node customIcon = nodeReader.getNullableNodeWithTag(links, "CustomIcon");
    if (customIcon == null) {
      return false;
    } else {
      NodeList customIcons = customIcon.getChildNodes();
      String iconUrl = nodeReader.getValueOfSimpleStringNullableNode(customIcons, "Url");

      return iconUrl != null && iconUrl.endsWith("suggestion-recording");
    }
  }

  @NotNull
  private Boolean isEpisodic(NodeList showAttributes, @Nullable String episodeIdentifier) throws BadlyFormattedXMLException {
    String detailUrl = getDetailUrl(showAttributes);

    try {
      Document document = readXMLFromTivoUrl(detailUrl, episodeIdentifier);

      debug("Checking against DB...");
      return parseDetailFromDocument(document);
    } catch (SAXException | IOException e) {
      e.printStackTrace();
      throw new RuntimeException("Error reading from URL: " + detailUrl);
    } catch (EpisodeAlreadyFoundException e) {
      e.printStackTrace();
      throw new RuntimeException("Episode already found.");
    } catch (BadlyFormattedXMLException e) {
      e.printStackTrace();
      throw new RuntimeException("XML formatting error!");
    }
  }


  private boolean parseDetailFromDocument(Document document) throws EpisodeAlreadyFoundException, BadlyFormattedXMLException {
    NodeList nodeList = document.getChildNodes();

    NodeList tvBus = nodeReader.getNodeWithTag(nodeList, "TvBusMarshalledStruct:TvBusEnvelope").getChildNodes();
    NodeList showing = nodeReader.getNodeWithTag(tvBus, "showing").getChildNodes();
    NodeList program = nodeReader.getNodeWithTag(showing, "program").getChildNodes();
    NodeList series = nodeReader.getNodeWithTag(program, "series").getChildNodes();

    String isEpisodic = nodeReader.getValueOfSimpleStringNullableNode(series, "isEpisodic");
    return Boolean.parseBoolean(isEpisodic);
  }


  private Boolean isRecordingNow(NodeList showAttributes) throws BadlyFormattedXMLException {
    NodeList links = nodeReader.getNodeWithTag(showAttributes, "Links").getChildNodes();
    Node customIcon = nodeReader.getNullableNodeWithTag(links, "CustomIcon");
    if (customIcon == null) {
      return false;
    } else {
      NodeList customIcons = customIcon.getChildNodes();
      String iconUrl = nodeReader.getValueOfSimpleStringNullableNode(customIcons, "Url");

      return iconUrl != null && iconUrl.endsWith("in-progress-recording");
    }
  }

  @Nullable
  private String getUrl(NodeList showAttributes) throws BadlyFormattedXMLException {
    NodeList links = nodeReader.getNodeWithTag(showAttributes, "Links").getChildNodes();
    NodeList content = nodeReader.getNodeWithTag(links, "Content").getChildNodes();
    return nodeReader.getValueOfSimpleStringNullableNode(content, "Url");
  }

  @Nullable
  private String getDetailUrl(NodeList showAttributes) throws BadlyFormattedXMLException {
    NodeList links = nodeReader.getNodeWithTag(showAttributes, "Links").getChildNodes();
    NodeList content = nodeReader.getNodeWithTag(links, "TiVoVideoDetails").getChildNodes();
    return nodeReader.getValueOfSimpleStringNullableNode(content, "Url");
  }

  private Document readXMLFromTivoUrl(String urlString, @Nullable String episodeIdentifier) throws IOException, SAXException {
    return tiVoDataProvider.connectAndRetrieveDocument(urlString, episodeIdentifier);
  }

  private Boolean isQuickUpdate() {
    return UpdateMode.QUICK.equals(updateMode);
  }

  private Boolean isFullUpdate() {
    return UpdateMode.FULL.equals(updateMode);
  }

  private void debug(Object message) {
    logger.debug(message);
  }

  @Override
  public String getRunnerName() {
    return "TiVo Communicator";
  }

  @Override
  public @Nullable UpdateMode getUpdateMode() {
    return updateMode;
  }


  private class EpisodeAlreadyFoundException extends Exception {
    public EpisodeAlreadyFoundException(String message) {
      super(message);
    }
  }

  private static class TiVoInfo {
    String programId;
    String tivoId;
    public String seriesTitle;
    String captureDate;
    Boolean isSuggestion;
    public String url;
    Boolean recordingNow = false;

    TiVoInfo(@NotNull NodeList showDetails, @NotNull NodeReader nodeReader) throws BadlyFormattedXMLException {
      programId = nodeReader.getValueOfSimpleStringNode(showDetails, "ProgramId");
      tivoId = nodeReader.getValueOfSimpleStringNode(showDetails, "SeriesId");
      seriesTitle = nodeReader.getValueOfSimpleStringNode(showDetails, "Title");
      captureDate = nodeReader.getValueOfSimpleStringNode(showDetails, "CaptureDate");
    }

  }
}

