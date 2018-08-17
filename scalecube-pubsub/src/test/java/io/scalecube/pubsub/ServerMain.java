package io.scalecube.pubsub;

import reactor.core.publisher.Flux;
import java.time.Duration;

public class ServerMain {


  public static void main(String[] args) {

    PubSub pubsub = PubSub.create();
    
    Flux<TopicPublisher> pub = pubsub.server("a", "224.0.1.1", 40456);

    pub.subscribe(p -> {
      Flux.interval(Duration.ofMillis(250)).subscribe(next->{
        p.next(PubSubMessage.builder().data("hello").build());
      });
    });
    
   
    System.out.println("done");
  }

  
}
