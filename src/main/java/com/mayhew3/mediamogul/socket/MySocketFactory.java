package com.mayhew3.mediamogul.socket;

public class MySocketFactory {

  public SocketWrapper createSocket(String socketEnv) {

    if (socketEnv.equals("mock")) {
      return new MockSocket();
    } else {
      return new SocketWrapperImpl(socketEnv);
    }

  }

}
