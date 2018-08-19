package io.scalecube.pubsub.example;

import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;
import static java.nio.charset.StandardCharsets.UTF_8;

public class Parser implements FragmentHandler {

	private String name;

	public Parser(String name) {
		this.name = name;
	}

	@Override
	public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header) {
		System.out.println(name);
		final byte[] buf = new byte[length];
		buffer.getBytes(offset, buf);

		System.out.println("received: [session " + String.format("%8x", Integer.valueOf(header.sessionId())) + " ] "
				+ "[stream " + String.format("%8x", Integer.valueOf(header.streamId())) + "] " + "[term"
				+ Integer.valueOf(header.termId()) + "] [" + new String(buf, UTF_8) + "]");
	}
}
