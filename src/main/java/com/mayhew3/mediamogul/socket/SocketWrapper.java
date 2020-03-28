package com.mayhew3.mediamogul.socket;

import org.json.JSONObject;

public interface SocketWrapper {

  void disconnect();
  void emit(String channel, JSONObject msg);

}
