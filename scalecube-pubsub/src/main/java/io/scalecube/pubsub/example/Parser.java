package io.scalecube.pubsub.example;

import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;
import static java.nio.charset.StandardCharsets.UTF_8;

public class Parser implements FragmentHandler {

  public Parser() {
  }

  @Override
  public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header) {
    final byte[] buf = new byte[length];
    buffer.getBytes(offset, buf);

    System.out.println("received: [session 0x{}] [stream 0x{}] [term 0x{}] {}" +
        String.format("%8x", Integer.valueOf(header.sessionId())) +
        String.format("%8x", Integer.valueOf(header.streamId()))+ 
        String.format("%8x", Integer.valueOf(header.termId())) + new String(buf, UTF_8));
  }
}
