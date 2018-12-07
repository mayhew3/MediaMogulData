package com.mayhew3.mediamogul.tv;

import com.mayhew3.postgresobject.db.SQLConnection;
import com.mayhew3.mediamogul.model.tv.*;
import info.debatty.java.stringsimilarity.NGram;
import info.debatty.java.stringsimilarity.interfaces.StringDistance;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.joda.time.DateTime;
import org.joda.time.DateTimeComparator;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class TVDBEpisodeMatcher {
  private SQLConnection connection;
  private TiVoEpisode tiVoEpisode;
  private Integer seriesId;

  private List<TVDBEpisode> allTVDBEpisodes = new ArrayList<>();
  private Set<Integer> existingPossibleMatches = new HashSet<>();

  private StringDistance distanceCalculator = new NGram();

  public TVDBEpisodeMatcher(SQLConnection connection, TiVoEpisode tiVoEpisode, Integer seriesId) {
    this.connection = connection;
    this.tiVoEpisode = tiVoEpisode;
    this.seriesId = seriesId;
  }

  public Optional<TVDBEpisode> matchAndLinkEpisode() throws SQLException {
    String episodeTitle = tiVoEpisode.title.getValue();
    Integer episodeNumber = tiVoEpisode.episodeNumber.getValue();
    Date startTime = tiVoEpisode.showingStartTime.getValue();

    if (episodeTitle == null) {
      tiVoEpisode.tvdbMatchStatus.changeValue(TVDBMatchStatus.NO_POSSIBLE_MATCH);
      tiVoEpisode.commit(connection);
      return Optional.empty();
    }

      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(
          "SELECT te.* " +
              "FROM episode e " +
              "INNER JOIN tvdb_episode te " +
              "  ON e.tvdb_episode_id = te.id " +
              "WHERE e.series_id = ? " +
              "AND te.retired = ? " +
              "AND e.retired = ? ",
          seriesId, 0, 0
      );

      while (resultSet.next()) {
        TVDBEpisode tvdbEpisode = new TVDBEpisode();
        tvdbEpisode.initializeFromDBObject(resultSet);
        allTVDBEpisodes.add(tvdbEpisode);
      }

      List<TVDBEpisode> exactTitleMatches = findExactTitleMatches(episodeTitle);
      List<TVDBEpisode> exactNumberMatches = findExactNumberMatches(episodeNumber);
      List<TVDBEpisode> exactDateMatches = findExactDateMatches(startTime);

      Set<TVDBEpisode> uniqueMatches = collectAllMatches(exactTitleMatches, exactNumberMatches, exactDateMatches);

      // todo: MM-328 change logic a bit: match complete if there is exactly one title match. If there are multiple, use
      // todo: date match to choose one, maybe with a needs confirmation? Date match should boost match score?
      if (exactlyOneMatch(exactTitleMatches, uniqueMatches)) {
        TVDBEpisode match = exactTitleMatches.get(0);
        matchEpisode(match);
        return Optional.of(match);
      } else if (oneMatchForTitleAndDate(exactTitleMatches, exactDateMatches)) {
        TVDBEpisode match = exactDateMatches.get(0);
        matchEpisode(match);
        return Optional.of(match);
      } else {
        collectBestMatches(episodeTitle);
        return Optional.empty();
      }
  }

  private boolean exactlyOneMatch(List<TVDBEpisode> exactTitleMatches, Set<TVDBEpisode> uniqueMatches) {
    return exactTitleMatches.size() == 1 && uniqueMatches.size() == 1;
  }

  private boolean oneMatchForTitleAndDate(List<TVDBEpisode> exactTitleMatches, List<TVDBEpisode> exactDateMatches) {
    return (exactDateMatches.size() == 1 && exactTitleMatches.contains(exactDateMatches.get(0))) ||
        (exactTitleMatches.size() == 1 && exactDateMatches.contains(exactTitleMatches.get(0)));
  }

  private void matchEpisode(@NotNull TVDBEpisode tvdbEpisode) throws SQLException {
    Episode episode = tvdbEpisode.getEpisode(connection);
    addToTiVoEpisodes(episode, tiVoEpisode);
  }

  private void addToTiVoEpisodes(Episode episode, TiVoEpisode tiVoEpisode) throws SQLException {
    List<TiVoEpisode> tiVoEpisodes = episode.getTiVoEpisodes(connection);
    if (!episode.hasMatch(tiVoEpisodes, tiVoEpisode.id.getValue())) {
      EdgeTiVoEpisode edgeTiVoEpisode = new EdgeTiVoEpisode();
      edgeTiVoEpisode.initializeForInsert();

      edgeTiVoEpisode.tivoEpisodeId.changeValue(tiVoEpisode.id.getValue());
      edgeTiVoEpisode.episodeId.changeValue(episode.id.getValue());

      edgeTiVoEpisode.commit(connection);
    }

    tiVoEpisode.tvdbMatchStatus.changeValue(TVDBMatchStatus.MATCH_COMPLETED);
    tiVoEpisode.commit(connection);

    episode.onTiVo.changeValue(true);
    episode.commit(connection);
  }

  private void collectBestMatches(String episodeTitle) throws SQLException {
    updateExistingPossibleMatches();

    TreeSet<PossibleEpisodeMatch> orderedBestTitleMatches = createOrderedTitleMatches(episodeTitle);
    List<PossibleEpisodeMatch> topFive = orderedBestTitleMatches.stream().limit(5).collect(Collectors.toList());

    for (PossibleEpisodeMatch possibleEpisodeMatch : topFive) {
      possibleEpisodeMatch.commit(connection);
    }

    if (!orderedBestTitleMatches.isEmpty()) {
      PossibleEpisodeMatch bestMatch = topFive.get(0);
      tiVoEpisode.tvdbMatchId.changeValue(bestMatch.tvdbEpisodeId.getValue());
      tiVoEpisode.tvdbMatchStatus.changeValue(TVDBMatchStatus.NEEDS_CONFIRMATION);
      tiVoEpisode.commit(connection);
    } else {
      tiVoEpisode.tvdbMatchStatus.changeValue(TVDBMatchStatus.NO_POSSIBLE_MATCH);
      tiVoEpisode.commit(connection);
    }
  }

  private TreeSet<PossibleEpisodeMatch> createOrderedTitleMatches(String episodeTitle) {
    TreeSet<PossibleEpisodeMatch> matches = new TreeSet<>();
    for (TVDBEpisode tvdbEpisode : allTVDBEpisodes) {
      if (tvdbEpisode.name.getValue() != null) {
        PossibleEpisodeMatch possibleEpisodeMatch = initializePossibleMatch(tvdbEpisode, episodeTitle);
        matches.add(possibleEpisodeMatch);
      }
    }
    return matches;
  }

  private PossibleEpisodeMatch initializePossibleMatch(TVDBEpisode tvdbEpisode, String episodeTitle) {
    Double distance = distanceCalculator.distance(episodeTitle, tvdbEpisode.name.getValue());

    Integer tvdbEpisodeId = tvdbEpisode.id.getValue();

    PossibleEpisodeMatch possibleEpisodeMatch = getOrCreateMatch(tvdbEpisodeId);

    possibleEpisodeMatch.seriesId.changeValue(seriesId);
    possibleEpisodeMatch.tivoEpisodeId.changeValue(tiVoEpisode.id.getValue());
    possibleEpisodeMatch.tvdbEpisodeId.changeValue(tvdbEpisodeId);
    possibleEpisodeMatch.matchScore.changeValue(distance);
    possibleEpisodeMatch.matchAlgorithm.changeValue("NGram");

    return possibleEpisodeMatch;
  }

  private void updateExistingPossibleMatches() {
    try {
      ResultSet resultSet = connection.prepareAndExecuteStatementFetch(
          "SELECT tvdb_episode_id " +
              "FROM possible_episode_match " +
              "WHERE tivo_episode_id = ? " +
              "AND retired = ? ",
          tiVoEpisode.id.getValue(),
          0);

      while (resultSet.next()) {
        Integer tvdb_episode_id = resultSet.getInt("tvdb_episode_id");
        existingPossibleMatches.add(tvdb_episode_id);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  private PossibleEpisodeMatch getOrCreateMatch(Integer tvdbEpisodeId) {
    try {
      PossibleEpisodeMatch possibleEpisodeMatch = new PossibleEpisodeMatch();

      if (existingPossibleMatches.contains(tvdbEpisodeId)) {

        ResultSet resultSet = connection.prepareAndExecuteStatementFetch(
            "SELECT * " +
                "FROM possible_episode_match " +
                "WHERE tivo_episode_id = ? " +
                "AND tvdb_episode_id = ? " +
                "AND retired = ? ",
            tiVoEpisode.id.getValue(),
            tvdbEpisodeId,
            0);

        if (resultSet.next()) {
          possibleEpisodeMatch.initializeFromDBObject(resultSet);
        } else {
          throw new IllegalStateException("Found possible match on first pass with TVDB ID " + tvdbEpisodeId + " and " +
              "TiVo ID " + tiVoEpisode.id.getValue() + ", but no longer found.");
        }
      } else {
        possibleEpisodeMatch.initializeForInsert();
      }
      return possibleEpisodeMatch;
    } catch (SQLException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @NotNull
  private Set<TVDBEpisode> collectAllMatches(List<TVDBEpisode> exactTitleMatches, List<TVDBEpisode> exactNumberMatches, List<TVDBEpisode> exactDateMatches) {
    Set<TVDBEpisode> uniqueMatches = new HashSet<>();
    uniqueMatches.addAll(exactTitleMatches);
    uniqueMatches.addAll(exactNumberMatches);
    uniqueMatches.addAll(exactDateMatches);
    return uniqueMatches;
  }

  private List<TVDBEpisode> findExactTitleMatches(String episodeTitle) {
    List<TVDBEpisode> matches = new ArrayList<>();

    if (episodeTitle != null) {
      for (TVDBEpisode tvdbEpisode : allTVDBEpisodes) {
        String tvdbTitleObject = tvdbEpisode.name.getValue();

        if (episodeTitle.equalsIgnoreCase(tvdbTitleObject)) {
          matches.add(tvdbEpisode);
        }
      }
    }
    return matches;
  }

  private List<TVDBEpisode> findExactNumberMatches(Integer episodeNumber) {
    List<TVDBEpisode> matches = new ArrayList<>();

    if (episodeNumber != null) {
      Integer seasonNumber = 1;

      if (episodeNumber < 100) {
        TVDBEpisode match = checkForNumberMatch(seasonNumber, episodeNumber);
        if (match != null) {
          matches.add(match);
        }
      } else {
        String episodeNumberString = episodeNumber.toString();
        int seasonLength = episodeNumberString.length() / 2;

        String seasonString = episodeNumberString.substring(0, seasonLength);
        String episodeString = episodeNumberString.substring(seasonLength, episodeNumberString.length());

        TVDBEpisode match = checkForNumberMatch(Integer.valueOf(seasonString), Integer.valueOf(episodeString));

        if (match != null) {
          matches.add(match);
        }
      }
    }

    return matches;
  }

  private List<TVDBEpisode> findExactDateMatches(Date startTime) {
    List<TVDBEpisode> matches = new ArrayList<>();

    if (startTime != null) {
      DateTime showingStartTime = new DateTime(startTime);

      for (TVDBEpisode tvdbEpisode : allTVDBEpisodes) {
        Date firstAiredValue = tvdbEpisode.firstAired.getValue();
        if (firstAiredValue != null) {
          DateTime firstAired = new DateTime(firstAiredValue);

          DateTimeComparator comparator = DateTimeComparator.getDateOnlyInstance();

          if (comparator.compare(showingStartTime, firstAired) == 0) {
            matches.add(tvdbEpisode);
          }
        }
      }
    }

    return matches;
  }

  @Nullable
  private TVDBEpisode checkForNumberMatch(Integer seasonNumber, Integer episodeNumber) {
    for (TVDBEpisode tvdbEpisode : allTVDBEpisodes) {
      Integer tvdbSeason = tvdbEpisode.seasonNumber.getValue();
      Integer tvdbEpisodeNumber = tvdbEpisode.episodeNumber.getValue();

      if (tvdbSeason == null || tvdbEpisodeNumber == null) {
        return null;
      }

      if (tvdbSeason.equals(seasonNumber) && tvdbEpisodeNumber.equals(episodeNumber)) {
        return tvdbEpisode;
      }
    }
    return null;
  }


}
