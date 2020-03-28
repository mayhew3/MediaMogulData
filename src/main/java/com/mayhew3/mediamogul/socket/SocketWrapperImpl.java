package com.mayhew3.mediamogul.socket;

import com.google.common.base.Preconditions;
import io.socket.client.IO;
import io.socket.client.Socket;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;

public class SocketWrapperImpl implements SocketWrapper {
  private Socket socket;

  private static int MAXIMUM_ATTEMPTS = 3;
  private static Logger logger = LogManager.getLogger(SocketWrapperImpl.class);

  protected SocketWrapperImpl(String socketEnv) {
    HashMap<String, String> socketUrls = new HashMap<>();
    socketUrls.put("local", "http://localhost:5000");
    socketUrls.put("heroku", "https://mediamogul.mayhew3.com/");

    Preconditions.checkArgument(socketUrls.containsKey(socketEnv));

    String uri = socketUrls.get(socketEnv);

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

      this.socket = socket;

    } catch (URISyntaxException e) {
      e.printStackTrace();
    }

  }

  private static void info(Object message) {
    logger.info(message);
  }

  public void disconnect() {
    socket.disconnect();
  }

  public void emit(String channel, JSONObject msg) {
    socket.emit(channel, msg);
  }
}
