package io.scalecube.pubsub;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.TopicProcessor;
import reactor.ipc.aeron.client.AeronClient;
import reactor.ipc.aeron.server.AeronServer;
import io.scalecube.pubsub.codec.MessageCodecException;
import io.scalecube.pubsub.codec.PubSubMessageCodec;
import java.util.concurrent.CompletableFuture;

public class PubSub {

  private static final PubSubMessageCodec codec = new PubSubMessageCodec();
  FluxProcessor<PubSubMessage, PubSubMessage> subject = TopicProcessor.<PubSubMessage>create().serialize();
  FluxSink<PubSubMessage> sink = subject.sink();

  public Mono<TopicSubscriber> client(String channel, String host, int port, int clientPort) {
    CompletableFuture<TopicSubscriber> topicFuture = new CompletableFuture<>();

    AeronClient client = AeronClient.create(channel, options -> {
      options.serverChannel("aeron:udp?endpoint=" + host + ":" + port);
      options.clientChannel("aeron:udp?endpoint=" + host + ":" + clientPort);
    });
    client.newHandler((inbound, outbound) -> {
      Flux<PubSubMessage> downstream = inbound.receive().map(bytes -> Unpooled.copiedBuffer(bytes)).map(byteBuf -> {
        try {
          return codec.decode(byteBuf);
        } catch (MessageCodecException e) {
          return PubSubMessage.error(e);
        }
      });
      topicFuture.complete(new TopicSubscriber(downstream));
      return Mono.never();
    }).subscribe();
    return Mono.fromFuture(topicFuture);
  }

  public Flux<TopicPublisher> server(String channel, String host, int port) {
    return Flux.create(emitter -> {
      AeronServer server = AeronServer.create(channel, options -> {
        options.serverChannel("aeron:udp?endpoint=" + host + ":" + port);
      });

      server.newHandler((inbound, outbound) -> {
        outbound.send(subject.map(msg -> {
          try {
            return codec.encode(msg);
          } catch (MessageCodecException e) {
            return null;
          }
        }).map(buf -> ((ByteBuf) buf).nioBuffer())).then().subscribe();

        emitter.next(new TopicPublisher(sink));
        return Mono.never();
      }).subscribe();
    });
  }

  public static PubSub create() {
    return new PubSub();
  }
}
