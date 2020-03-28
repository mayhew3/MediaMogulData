package com.mayhew3.mediamogul.socket;

public class MySocketFactory {

  public SocketWrapper createSocket(String socketEnv, String appRole) {

    if (socketEnv.equals("mock")) {
      return new MockSocket();
    } else {
      return new SocketWrapperImpl(socketEnv, appRole);
    }

  }

}
