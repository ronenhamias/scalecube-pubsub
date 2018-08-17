package io.scalecube.pubsub.example.runners;

import io.scalecube.pubsub.example.AeronClient;

public class ClientRunner {

  public static void main(String[] args) {
    AeronClient.builder().start();
  }

}
