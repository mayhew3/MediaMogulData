package com.mayhew3.mediamogul.tv.helper;

public enum TVDBApprovalStatus {
  APPROVED("approved"),
  REJECTED("rejected"),
  PENDING("pending");

  private final String typeKey;

  TVDBApprovalStatus(String dbString) {
    this.typeKey = dbString;
  }

  public String getTypeKey() {
    return typeKey;
  }

  @Override
  public String toString() {
    return getTypeKey();
  }
}
