package io.scalecube.pubsub.example.runners;

import java.util.UUID;
import reactor.ipc.aeron.ControlMessageSubscriber;

public class ExampleControlSubscriber implements ControlMessageSubscriber {
  @Override
  public void onSubscribe(org.reactivestreams.Subscription subscription) {
    System.out.println("onSubscribe: " + subscription);
  }

  @Override
  public void onHeartbeat(long sessionId) {
    System.out.println("onHeartbeat: " + sessionId);
  }

  @Override
  public void onConnectAck(UUID connectRequestId, long sessionId, int serverSessionStreamId) {
    System.out.println("onHeartbeat: " + sessionId);
  }

  @Override
  public void onConnect(UUID connectRequestId, String clientChannel, int clientControlStreamId,
      int clientSessionStreamId) {
    System.out.println("onConnect: " + connectRequestId);
  }
}
