package com.mayhew3.mediamogul.socket;

import com.google.common.base.Preconditions;
import io.socket.client.Socket;
import org.json.JSONObject;

public class SocketWrapper {
  private Socket socket;
  private boolean isMock;

  protected SocketWrapper(boolean isMock, Socket socket) {
    Preconditions.checkArgument(isMock || socket != null);
    this.isMock = isMock;
    this.socket = socket;
  }


  public void disconnect() {
    if (!isMock) {
      socket.disconnect();
    }
  }

  public void emit(String channel, JSONObject msg) {
    if (!isMock) {
      socket.emit(channel, msg);
    }
  }
}
