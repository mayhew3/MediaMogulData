package com.mayhew3.mediamogul.tv.helper;

import com.google.common.collect.Lists;
import com.mayhew3.postgresobject.ArgumentChecker;

import java.util.Optional;

public enum UpdateMode {
  FULL("Full"),
  SMART("Smart"),
  FEW_ERRORS("FewErrors"),
  OLD_ERRORS("OldErrors"),
  SINGLE("Single"),
  RECENT("Recent"),
  QUICK("Quick"),
  AIRTIMES("AirTimes"),
  SANITY("Sanity"),
  SERVICE("Service"),
  EPISODE_MATCH("EpisodeMatch"),
  FIRST_PASS("FirstPass"),
  MANUAL("Manual"),
  UNMATCHED("Unmatched");

  private final String typekey;

  UpdateMode(String dbString) {
    this.typekey = dbString;
  }

  public String getTypekey() {
    return typekey;
  }

  public static Optional<UpdateMode> getUpdateType(final String typekey) {
    return Lists.newArrayList(UpdateMode.values())
        .stream()
        .filter(updateMode -> updateMode.typekey.equalsIgnoreCase(typekey))
        .findAny();
  }

  public static UpdateMode getUpdateModeOrDefault(ArgumentChecker checker, UpdateMode defaultType) {
    Optional<String> modeIdentifier = checker.getUpdateModeIdentifier();
    if (modeIdentifier.isPresent()) {
      Optional<UpdateMode> optionalType = getUpdateType(modeIdentifier.get());
      if (optionalType.isPresent()) {
        return optionalType.get();
      } else {
        throw new IllegalArgumentException("No Update Mode found: " + modeIdentifier);
      }
    } else {
      return defaultType;
    }
  }

  @Override
  public String toString() {
    return getTypekey();
  }
}
