package io.scalecube.pubsub.example;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;
import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.ExclusivePublication;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MediaDriver.Context;
import io.scalecube.pubsub.example.AeronServer.Builder;
import org.agrona.BufferUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
  private UnsafeBuffer buffer;
  private FragmentAssembler fragmentAssembler;
  private String aeronDirectoryName;

  public AeronClient(Builder builder) {

    this.localAddress = builder.localAddress;
    this.localPort = builder.localPort;
    this.serverAddress = builder.serverAddress;
    this.serverControlPort = builder.serverControlPort;
    this.serverDataPort = builder.serverDataPort;
    this.aeronDirectoryName = builder.aeronDirectoryName;

    this.mediaContext =
        new MediaDriver.Context().dirDeleteOnStart(true).aeronDirectoryName(this.aeronDirectoryName + localPort);
    this.media = MediaDriver.launch(this.mediaContext);

    this.aeronContext = new Aeron.Context().aeronDirectoryName(this.aeronDirectoryName + localPort);
    this.aeron = Aeron.connect(this.aeronContext);

    this.buffer = new UnsafeBuffer(BufferUtil.allocateDirectAligned(2048, 16));
    this.fragmentAssembler = builder.fragmentAssembler;

  }

  public void start() {
    /*
     * Create a subscription to read data from the server. This uses dynamic MDC to will send messages
     * to the server's control port, and the server will react by data to the local address and port
     * combination specified here.
     */
    final String sub_uri = new ChannelUriStringBuilder().mtu(Shared.MTU).reliable(Boolean.TRUE).media("udp")
        .endpoint(this.localAddress + ":" + this.localPort)
        .controlEndpoint(this.serverAddress + ":" + this.serverControlPort).controlMode("dynamic").build();

    final Subscription sub =
        this.aeron.addSubscription(sub_uri, Shared.STREAM_ID, this::onImageAvailable, this::onImageUnavailable);

    /*
     * Create a publication for sending data to the server.
     */
    final String pub_uri = new ChannelUriStringBuilder().mtu(Shared.MTU).reliable(Boolean.TRUE).media("udp")
        .endpoint(this.serverAddress + ":" + this.serverDataPort).build();

    final ExclusivePublication pub = this.aeron.addExclusivePublication(pub_uri, Shared.STREAM_ID);

    ExecutorService service = Executors.newSingleThreadExecutor();
    service.execute(() -> {
      while (true) {
        if (sub.isConnected()) {
          sub.poll(this.fragmentAssembler, 10);
        }

        if (pub.isConnected()) {
          send(pub, this.buffer, clientMessage());
        }

        try {
          Thread.sleep(2000L);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    });
  }

  static void send(final Publication pub, final MutableDirectBuffer buffer, final String message) {

    final byte[] value = message.getBytes(UTF_8);
    buffer.putBytes(0, value);
    final long result = pub.offer(buffer, 0, value.length);

    if (result < 0L) {
      // log.error("could not send: {}", Long.valueOf(result));
    }
  }

  private void onImageUnavailable(final Image image) {
    System.out.println(
        "onImageUnavailable: " + String.format("%08x", Integer.valueOf(image.sessionId())) + image.sourceIdentity());
  }

  private void onImageAvailable(final Image image) {
    System.out.println(
        "onImageAvailable: " + String.format("%08x", Integer.valueOf(image.sessionId())) + image.sourceIdentity());
  }

  private String clientMessage() {
    return new StringBuilder(128).append("Client HELLO: ").append(LocalDateTime.now().format(ISO_LOCAL_DATE_TIME))
        .toString();
  }

  static public Builder builder() {
    return new Builder();
  }

  static public class Builder {
    public String aeronDirectoryName = "/dev/shm/aeron-client-";
    public int serverDataPort = 9091;
    public int serverControlPort = 9090;
    public String serverAddress = "localhost";
    public int localPort = 8080;
    public String localAddress = "localhost";
    private FragmentAssembler fragmentAssembler;

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
  }
}
