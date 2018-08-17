package io.scalecube.pubsub;

import reactor.core.publisher.Mono;

public class ClientMain {

  public static void main(String[] args) throws InterruptedException {
    
    PubSub pubsub = PubSub.create();
    
    Mono<TopicSubscriber> sub1 = pubsub.client("1","224.0.1.1", 40456, 13002);
    
    sub1.subscribe(x -> x.listen().subscribe(msg -> {
      System.out.println("1: " + msg);
    }));
    
    Thread.currentThread().join();
  }

}
