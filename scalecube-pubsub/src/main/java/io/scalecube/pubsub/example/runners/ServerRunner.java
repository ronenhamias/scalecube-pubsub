package io.scalecube.pubsub.example.runners;

import io.scalecube.pubsub.example.AeronServer;

public class ServerRunner {

  public static void main(String[] args) {
    final AeronServer s = AeronServer.builder().start();
  }

}
