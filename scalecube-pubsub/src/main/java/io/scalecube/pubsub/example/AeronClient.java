package io.scalecube.pubsub.example;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.ExclusivePublication;
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
		this.mediaContext = new MediaDriver.Context().dirDeleteOnStart(true)
				.aeronDirectoryName(this.aeronDirectoryName + localPort);
		this.media = MediaDriver.launch(this.mediaContext);

		this.aeronContext = new Aeron.Context().aeronDirectoryName(this.aeronDirectoryName + localPort);
		this.aeron = Aeron.connect(this.aeronContext);
		this.fragmentAssembler = builder.fragmentAssembler;
	}

	public void start(FragmentAssembler assembler) {
		this.pooler = new AeronEventLoop("aeron-event-loop");
		this.pooler.initialise();
	}

	public void addControlSubscription(ControlMessageSubscriber controlMessageSubscriber) {
		/*
		 * Create a subscription to read data from the server. This uses dynamic MDC to
		 * will send messages to the server's control port, and the server will react by
		 * data to the local address and port combination specified here.
		 */
		final String sub_uri = new ChannelUriStringBuilder().mtu(Shared.MTU).reliable(Boolean.TRUE).media("udp")
				.endpoint(this.localAddress + ":" + this.localPort)
				.controlEndpoint(this.serverAddress + ":" + this.serverControlPort).controlMode("dynamic").build();

		/**
		 * create control subscription.
		 */
		final Subscription controlSubscription = this.aeron.addSubscription(sub_uri, Shared.STREAM_ID,
				this::onImageAvailable, this::onImageUnavailable);

		this.pooler.addControlSubscription(controlSubscription, controlMessageSubscriber);

	}

	/*
	 * Create a subscription to read data from the server. This uses dynamic MDC to
	 * will send messages to the server's control port, and the server will react by
	 * data to the local address and port combination specified here.
	 */
	public Subscription addDataSubscription(String channel, int streamId, DataMessageSubscriber dataMessageSubscriber) {

//    final String sub_uri = new ChannelUriStringBuilder().mtu(Shared.MTU).reliable(Boolean.TRUE).media("udp")
//        .endpoint(this.localAddress + ":" + this.localPort)
//        .controlEndpoint(this.serverAddress + ":" + this.serverDataPort).controlMode("dynamic").build();

		final Subscription sub = this.aeron.addSubscription(channel, streamId, this::onImageAvailable,
				this::onImageUnavailable);

		this.pooler.addDataSubscription(sub, dataMessageSubscriber);
		return sub;
	}

	private void onImageUnavailable(final Image image) {
		System.out.println("onImageUnavailable: " + String.format("%08x", Integer.valueOf(image.sessionId()))
				+ image.sourceIdentity());
	}

	private void onImageAvailable(final Image image) {
		System.out.println("onImageAvailable: " + String.format("%08x", Integer.valueOf(image.sessionId()))
				+ image.sourceIdentity());
	}

	static public Builder builder() {
		return new Builder();
	}

	static public class Builder {
		private String aeronDirectoryName = "/scalecube/media/aeron-";
		private String serverAddress = "localhost";
		private String localAddress = "localhost";

		private int serverDataPort = 9091;
		private int serverControlPort = 9090;

		private int localPort = 8080;
		
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
			client.start(this.fragmentAssembler);
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
		private AeronClient client;
		private int streamId = Shared.STREAM_ID;

		ChannelUriStringBuilder channel;

		private PublicationBuilder(AeronClient client, String remoteAddress) {
			this.client = client;
			this.channel = new ChannelUriStringBuilder()
					.mtu(Shared.MTU)
					.reliable(Boolean.TRUE)
					.media("udp")
					.endpoint(remoteAddress);
		}

		public Publication build() {
			System.out.println("publish uri: " + channel.build());
			ExclusivePublication pub = this.client.aeron.addExclusivePublication(channel.build(), streamId);
			return pub;
		}
	}

	public class SubscriptionBuilder {
		private AeronClient client;
		private int streamId = Shared.STREAM_ID;

		ChannelUriStringBuilder channel;

		private SubscriptionBuilder(AeronClient client, String localAddress, String remoteAddress) {
			this.client = client;
			this.channel = new ChannelUriStringBuilder()
					.mtu(Shared.MTU)
					.reliable(Boolean.TRUE)
					.media("udp")
					.endpoint(localAddress)
					.controlEndpoint(remoteAddress)
					.controlMode("dynamic");
		}

		public Subscription subscribe(DataMessageSubscriber dataMessageSubscriber) {
			System.out.println("subscribe uri: " + channel.build());
			return this.client.addDataSubscription(channel.build(), streamId, dataMessageSubscriber);
		}
	}

	/*
	 * Create a publication for sending data to the server.
	 */
	public PublicationBuilder publication() {
		return new PublicationBuilder(this, this.serverAddress + ":" + this.serverDataPort);
	}

	/*
	 * Create a publication for sending data to the server.
	 */
	public SubscriptionBuilder subscription() {
		String listenEndpoint = this.localAddress + ":" + this.localPort;
		String sendEndpoint = this.serverAddress + ":" + this.serverDataPort;
		return new SubscriptionBuilder(this, listenEndpoint, sendEndpoint);
	}

	public void send(Publication pub, UnsafeBuffer buffer, String clientMessage) {
		Utilities.send(pub, buffer, clientMessage);
	}

}
