package com.mayhew3.mediamogul;

import io.socket.client.IO;
import io.socket.client.Socket;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;

public class MySocketFactory {
  private static Logger logger = LogManager.getLogger(MySocketFactory.class);

  private static int MAXIMUM_ATTEMPTS = 3;

  public Socket createSocket(String socketEnv) {
    HashMap<String, String> socketUrls = new HashMap<>();
    socketUrls.put("local", "http://localhost:5000");
    socketUrls.put("heroku", "https://mediamogul.mayhew3.com/");

    String uri = socketUrls.get(socketEnv);
    if (uri == null) {
      throw new RuntimeException("Invalid envName: " + socketEnv);
    }

    try {
      Socket socket = IO.socket(uri);

      socket.on(Socket.EVENT_CONNECT_ERROR, args -> info("Error connecting to socket server! Args: " + Arrays.toString(args)));

      socket.on(Socket.EVENT_DISCONNECT, args -> info("Disconnect event! Args: " + Arrays.toString(args)));

      socket.on(Socket.EVENT_RECONNECT, args -> info("Reconnect event! Args: " + Arrays.toString(args)));

      socket.on(Socket.EVENT_RECONNECT_ATTEMPT, args -> {
        Integer attemptNumber = (Integer)args[0];
        info("Reconnect Attempt event! Attempt #" + attemptNumber);
        if (attemptNumber > MAXIMUM_ATTEMPTS) {
          throw new IllegalStateException("Maximum connection attempts exceeded. Quitting.");
        }
      });

      socket.on(Socket.EVENT_CONNECT, args -> {
        info("Connected to socket server!");
      });

      socket.connect();
      return socket;
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  private static void info(Object message) {
    logger.info(message);
  }

}
