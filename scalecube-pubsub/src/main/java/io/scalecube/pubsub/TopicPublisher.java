package io.scalecube.pubsub;

import reactor.core.publisher.FluxSink;

public class TopicPublisher {

  private FluxSink<PubSubMessage> sink;

  public TopicPublisher(FluxSink<PubSubMessage> sink) {
    this.sink = sink;
  }

  public void next(PubSubMessage message) {
    sink.next(message);
  }
}
