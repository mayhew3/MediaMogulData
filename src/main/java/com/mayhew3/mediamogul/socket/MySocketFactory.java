package com.mayhew3.mediamogul.socket;

public class MySocketFactory {

  public SocketWrapper createSocket(String socketEnv, String envName) {

    if (socketEnv.equals("mock")) {
      return new MockSocket();
    } else {
      return new SocketWrapperImpl(socketEnv, envName);
    }

  }

}
