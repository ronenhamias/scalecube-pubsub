package io.scalecube.pubsub.example.runners;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;
import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.scalecube.pubsub.example.AeronClient;
import io.scalecube.pubsub.example.Parser;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.LocalDateTime;
import org.agrona.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.ipc.aeron.DataMessageSubscriber;

public class ClientRunner {

  public static void main(String[] args) {
    final UnsafeBuffer buffer = new UnsafeBuffer(BufferUtil.allocateDirectAligned(2048, 16));

    AeronClient client = AeronClient.builder()
        .fragmentAssembler(new FragmentAssembler(new Parser("client")))
        .addControlSubscription(new ExampleControlSubscriber())
        .start();

    client.subscription().subscribe(new DataMessageSubscriber() {

      @Override
      public void onSubscribe(Subscription subscription) {
        System.out.println("onSubscribe");
      }

      @Override
      public void onNext(long sessionId, ByteBuffer buffer) {
        System.out.println("onNext");
      }

      @Override
      public void onComplete(long sessionId) {
        System.out.println("onComplete");
      }
    });

    Publication pub = client.publication().build();
    Flux.interval(Duration.ofMillis(1000)).subscribe(consumer->{
    	client.send(pub, buffer, clientMessage());
    });
  }

  private static String clientMessage() {
    return new StringBuilder(128).append("Client HELLO: ").append(LocalDateTime.now().format(ISO_LOCAL_DATE_TIME))
        .toString();
  }
}
