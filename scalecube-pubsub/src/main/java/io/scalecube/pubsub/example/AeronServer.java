package io.scalecube.pubsub.example;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;
import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.ExclusivePublication;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.agrona.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

public class AeronServer {
	private final Logger log;
	private final MediaDriver.Context mediaContext;
	private final MediaDriver media;
	private final Aeron.Context aeronContext;
	private final Aeron aeron;
	private final UnsafeBuffer buffer;
	private final FragmentAssembler fragmentAssembler;
	private final String localAddress;
	private final int localControlPort;
	private final int localDataPort;
	private String aeronDirectoryName;
	private Publication pub;

	public AeronServer(Builder builder) {

		this.log = LoggerFactory.getLogger("Server");

		this.localAddress = builder.address;
		this.localControlPort = builder.controlPort;
		this.localDataPort = builder.dataPort;
		this.aeronDirectoryName = builder.aeronDirectoryName;

		this.mediaContext = new MediaDriver.Context().dirDeleteOnStart(true)
				.aeronDirectoryName(this.aeronDirectoryName);
		this.media = MediaDriver.launch(this.mediaContext);

		this.aeronContext = new Aeron.Context().aeronDirectoryName(this.aeronDirectoryName);
		this.aeron = Aeron.connect(this.aeronContext);

		this.buffer = new UnsafeBuffer(BufferUtil.allocateDirectAligned(2048, 16));

		// frame handler and decoder:
		this.fragmentAssembler = builder.fragmentAssembler;
	}

	private static String serverMessage() {
		return new StringBuilder(128).append("Server HELLO: ").append(LocalDateTime.now().format(ISO_LOCAL_DATE_TIME))
				.toString();
	}

	public static class Builder {

		public FragmentAssembler fragmentAssembler;
		public String aeronDirectoryName = "/dev/sc/aeron-server";
		int dataPort = 9091;
		int controlPort = 9090;
		String address = "localhost";

		public Builder dataPort(int dataPort) {
			this.dataPort = dataPort;
			return this;
		}

		public Builder controlPort(int controlPort) {
			this.controlPort = controlPort;
			return this;
		}

		public Builder address(String address) {
			this.address = address;
			return this;
		}

		public AeronServer start() {
			AeronServer server = new AeronServer(this);
			server.start();
			return server;
		}

		public Builder fragmentAssembler(FragmentAssembler fragmentAssembler) {
			this.fragmentAssembler = fragmentAssembler;
			return this;
		}
	}

	public static Builder builder() {
		return new Builder();
	}

	public void start() {

	    final String pub_uri =
	    	      new ChannelUriStringBuilder()
	    	        .mtu(Shared.MTU)
	    	        .reliable(Boolean.TRUE)
	    	        .media("udp")
	    	        .endpoint(this.localAddress + ":" + 8080)
	    	        .build();
	    
	    pub = this.aeron.addPublication(pub_uri, Shared.STREAM_ID);
	        
	    
		final String sub_uri = new ChannelUriStringBuilder().mtu(Shared.MTU).reliable(Boolean.TRUE).media("udp")
				.endpoint(this.localAddress + ":" + this.localDataPort).build();
		final Subscription sub = this.aeron.addSubscription(sub_uri, Shared.STREAM_ID, this::onImageAvailable,
				this::onImageUnavailable);

		
		ExecutorService service = Executors.newSingleThreadExecutor();
		service.execute(() -> {
			while (true) {
				if (pub.isConnected()) {
					Flux.interval(Duration.ofMillis(1000)).subscribe(i -> {
						Utilities.send(pub, this.buffer, serverMessage());
					});
				}
				if (sub.isConnected()) {
					sub.poll(this.fragmentAssembler, 10);
					Flux.interval(Duration.ofMillis(1000)).subscribe(i -> {
						Utilities.send(pub, this.buffer, serverMessage());
					});
				}
				try {
					Thread.sleep(1000L);
				} catch (final InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
		});
		
	}

	private void onImageUnavailable(final Image image) {
		System.out.println("onImageUnavailable: " + String.format("%08x", Integer.valueOf(image.sessionId()))
				+ image.sourceIdentity());
		
	}

	private void onImageAvailable(final Image image) {
		System.out.println("onImageAvailable: " + String.format("%08x", Integer.valueOf(image.sessionId()))
				+ image.sourceIdentity());
	}
}
