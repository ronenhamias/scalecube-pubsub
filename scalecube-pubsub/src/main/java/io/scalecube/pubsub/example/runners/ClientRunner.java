package io.scalecube.pubsub.example.runners;

import io.aeron.FragmentAssembler;
import io.scalecube.pubsub.example.AeronClient;
import io.scalecube.pubsub.example.Parser;

public class ClientRunner {

  public static void main(String[] args) {
    AeronClient.builder()
      .fragmentAssembler(new FragmentAssembler(new Parser()))
      .start();
  }

}
