package org.apache.hadoop.security.sasl;

public enum TqSaslState {
  SUCCESS(0),
  NEGOTIATE(1),
  INITIATE(2),
  CHALLENGE(3),
  RESPONSE(4);

  private final int index;

  TqSaslState(int index) {
    this.index = index;
  }

  public int getIndex() {
    return index;
  }

  public static TqSaslState valueOf(int index) {
    switch (index) {
      case 0:
        return SUCCESS;
      case 1:
        return NEGOTIATE;
      case 2:
        return INITIATE;
      case 3:
        return CHALLENGE;
      case 4:
        return RESPONSE;
      default:
        return null;
    }
  }
}
