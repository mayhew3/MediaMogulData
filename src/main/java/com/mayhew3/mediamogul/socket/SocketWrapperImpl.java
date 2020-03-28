package com.mayhew3.mediamogul.socket;

import com.google.common.base.Preconditions;
import io.socket.client.Socket;
import org.json.JSONObject;

public class SocketWrapperImpl implements SocketWrapper {
  private Socket socket;

  protected SocketWrapperImpl(Socket socket) {
    Preconditions.checkArgument(socket != null);
    this.socket = socket;
  }

  public void disconnect() {
    socket.disconnect();
  }

  public void emit(String channel, JSONObject msg) {
    socket.emit(channel, msg);
  }
}
