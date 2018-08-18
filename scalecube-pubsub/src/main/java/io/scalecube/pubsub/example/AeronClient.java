package io.scalecube.pubsub.example;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MediaDriver.Context;
import org.agrona.concurrent.UnsafeBuffer;
import reactor.ipc.aeron.ControlMessageSubscriber;
import reactor.ipc.aeron.DataMessageSubscriber;

public class AeronClient {

  private String localAddress;
  private int localPort;

  private String serverAddress;
  private int serverControlPort;
  private int serverDataPort;
  private Context mediaContext;
  private MediaDriver media;
  private Aeron.Context aeronContext;
  private Aeron aeron;

  private FragmentAssembler fragmentAssembler;
  private String aeronDirectoryName;
  private AeronEventLoop pooler;
  private ControlMessageSubscriber controlSubscriber;
  private DataMessageSubscriber dataSubscriber;

  public AeronClient(Builder builder) {

    this.localAddress = builder.localAddress;
    this.localPort = builder.localPort;
    this.serverAddress = builder.serverAddress;
    this.serverControlPort = builder.serverControlPort;
    this.serverDataPort = builder.serverDataPort;
    this.aeronDirectoryName = builder.aeronDirectoryName;
    this.controlSubscriber = builder.controlMessageSubscriber;
    this.mediaContext =
        new MediaDriver.Context().dirDeleteOnStart(true).aeronDirectoryName(this.aeronDirectoryName + localPort);
    this.media = MediaDriver.launch(this.mediaContext);

    this.aeronContext = new Aeron.Context().aeronDirectoryName(this.aeronDirectoryName + localPort);
    this.aeron = Aeron.connect(this.aeronContext);
    this.fragmentAssembler = builder.fragmentAssembler;
  }

  public void start() {
    this.pooler = new AeronEventLoop("aeron-event-loop");
    this.addControlSubscription(this.controlSubscriber);
    this.addDataSubscription(this.dataSubscriber);
    this.pooler.initialise();
  }

  public void addControlSubscription(ControlMessageSubscriber controlMessageSubscriber) {
    /*
     * Create a subscription to read data from the server. This uses dynamic MDC to will send messages
     * to the server's control port, and the server will react by data to the local address and port
     * combination specified here.
     */
    final String sub_uri = new ChannelUriStringBuilder().mtu(Shared.MTU).reliable(Boolean.TRUE).media("udp")
        .endpoint(this.localAddress + ":" + this.localPort)
        .controlEndpoint(this.serverAddress + ":" + this.serverControlPort).controlMode("dynamic").build();

    /**
     * create control subscription.
     */
    final Subscription controlSubscription =
        this.aeron.addSubscription(sub_uri, Shared.STREAM_ID, this::onImageAvailable, this::onImageUnavailable);

    this.pooler.addControlSubscription(controlSubscription, controlMessageSubscriber);

  }

  public void addDataSubscription(DataMessageSubscriber dataMessageSubscriber) {
    /*
     * Create a subscription to read data from the server. This uses dynamic MDC to will send messages
     * to the server's control port, and the server will react by data to the local address and port
     * combination specified here.
     */
    final String sub_uri = new ChannelUriStringBuilder().mtu(Shared.MTU).reliable(Boolean.TRUE).media("udp")
        .endpoint(this.localAddress + ":" + this.localPort)
        .controlEndpoint(this.serverAddress + ":" + this.serverDataPort).controlMode("dynamic").build();

    /**
     * create control subscription.
     */
    final Subscription controlSubscription =
        this.aeron.addSubscription(sub_uri, Shared.STREAM_ID, this::onImageAvailable, this::onImageUnavailable);

    this.pooler.addDataSubscription(controlSubscription, dataMessageSubscriber);

  }

  private void onImageUnavailable(final Image image) {
    System.out.println(
        "onImageUnavailable: " + String.format("%08x", Integer.valueOf(image.sessionId())) + image.sourceIdentity());
  }

  private void onImageAvailable(final Image image) {
    System.out.println(
        "onImageAvailable: " + String.format("%08x", Integer.valueOf(image.sessionId())) + image.sourceIdentity());
  }

  static public Builder builder() {
    return new Builder();
  }

  static public class Builder {
    public String aeronDirectoryName = "/dev/sc/aeron-client-";
    public int serverDataPort = 9091;
    public int serverControlPort = 9090;
    public String serverAddress = "localhost";
    public int localPort = 8080;
    public String localAddress = "localhost";
    private FragmentAssembler fragmentAssembler;
    private ControlMessageSubscriber controlMessageSubscriber;

    public AeronClient start(Builder builder) {
      return new AeronClient(this);
    }

    public Builder serverDataPort(int serverDataPort) {
      this.serverDataPort = serverDataPort;
      return this;
    }

    public Builder serverControlPort(int serverControlPort) {
      this.serverControlPort = serverControlPort;
      return this;
    }

    public Builder localPort(int localPort) {
      this.localPort = localPort;
      return this;
    }

    public Builder serverAddress(String serverAddress) {
      this.serverAddress = serverAddress;
      return this;
    }

    public Builder localAddress(String localAddress) {
      this.localAddress = localAddress;
      return this;
    }

    public AeronClient start() {
      AeronClient client = new AeronClient(this);
      client.start();
      return client;
    }

    public Builder fragmentAssembler(FragmentAssembler fragmentAssembler) {
      this.fragmentAssembler = fragmentAssembler;
      return this;
    }

    public Builder addControlSubscription(ControlMessageSubscriber controlMessageSubscriber) {
      this.controlMessageSubscriber = controlMessageSubscriber;
      return this;
    }
  }

  public class PublicationBuilder {
    private Aeron aeron;
    private int streamId = Shared.STREAM_ID;
    private String endpoint;

    ChannelUriStringBuilder channel =
        new ChannelUriStringBuilder().mtu(Shared.MTU).reliable(Boolean.TRUE).media("udp").endpoint(endpoint);


    private PublicationBuilder(Aeron aeron, String endpoint) {
      this.aeron = aeron;
      this.endpoint = endpoint;
    }

    public Publication build() {
      return this.aeron.addExclusivePublication(channel.build(), streamId);
    }
  }

  /*
   * Create a publication for sending data to the server.
   */
  public PublicationBuilder publication() {
    return new PublicationBuilder(this.aeron, this.serverAddress + ":" + this.serverDataPort);
  }

  public void send(Publication pub, UnsafeBuffer buffer, String clientMessage) {
    Utilities.send(pub, buffer, clientMessage);
  }

}
