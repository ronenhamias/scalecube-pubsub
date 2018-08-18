package io.scalecube.pubsub.example.runners;

import io.aeron.FragmentAssembler;
import io.scalecube.pubsub.example.AeronServer;
import io.scalecube.pubsub.example.Parser;

public class ServerRunner {

  public static void main(String[] args) {
    AeronServer.builder()
      .fragmentAssembler(new FragmentAssembler(new Parser())).start();
  }

}
