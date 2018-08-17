package io.scalecube.pubsub;

import reactor.core.publisher.Flux;

public class TopicSubscriber {

  private Flux<PubSubMessage> downstream;

  public TopicSubscriber(Flux<PubSubMessage> downstream) {
    this.downstream = downstream;
  }

  public Flux<PubSubMessage> listen() {
    return downstream;
  }
}
