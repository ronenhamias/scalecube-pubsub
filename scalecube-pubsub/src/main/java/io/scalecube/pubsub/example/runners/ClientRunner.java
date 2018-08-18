package io.scalecube.pubsub.example.runners;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;
import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.scalecube.pubsub.example.AeronClient;
import io.scalecube.pubsub.example.Parser;
import java.time.LocalDateTime;
import org.agrona.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;

public class ClientRunner {

  public static void main(String[] args) {
    final UnsafeBuffer buffer = new UnsafeBuffer(BufferUtil.allocateDirectAligned(2048, 16));

    AeronClient client = AeronClient.builder()
        .fragmentAssembler(new FragmentAssembler(new Parser()))
        .addControlSubscription(new ExampleControlSubscriber())
        .start();
    
    Publication pub = client.publication().build();
    client.send(pub, buffer, clientMessage());
  }

  private static String clientMessage() {
    return new StringBuilder(128).append("Client HELLO: ")
        .append(LocalDateTime.now().format(ISO_LOCAL_DATE_TIME)).toString();
  }
}
