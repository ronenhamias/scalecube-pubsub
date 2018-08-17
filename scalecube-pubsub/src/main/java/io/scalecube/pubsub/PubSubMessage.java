package io.scalecube.pubsub;

import io.scalecube.pubsub.codec.MessageCodecException;
import java.util.Objects;

public class PubSubMessage {

  public static final String QUALIFIER_FIELD = "q";
  public static final String STREAM_ID_FIELD = "sid";
  public static final String DATA_FIELD = "d";

  private String qualifier;
  private Long streamId;
  private Object data;

  /**
   * Get a builder by pattern form given {@link PubSubMessage}.
   *
   * @param msg Message form where to copy field values.
   * @return builder with fields copied from given {@link PubSubMessage}
   */
  public static Builder from(PubSubMessage msg) {
    Builder builder = new Builder();
    builder.qualifier = msg.qualifier();
    builder.streamId = msg.streamId();
    builder.data = msg.data();
    return builder;
  }

  PubSubMessage() {}

  private PubSubMessage(String qualifier, Long streamId, Object data) {
    this.qualifier = qualifier;
    this.streamId = streamId;
    this.data = data;
  }

  public static Builder builder() {
    return new Builder();
  }

  public String qualifier() {
    return qualifier;
  }

  public Long streamId() {
    return streamId;
  }

  public <T> T data() {
    // noinspection unchecked
    return (T) data;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("GatewayMessage{");
    sb.append("qualifier='").append(qualifier).append('\'');
    sb.append(", streamId=").append(streamId);
    sb.append(", data=").append(data);
    sb.append('}');
    return sb.toString();
  }

  public static class Builder {

    private String qualifier;
    private Long streamId;
    private Object data;

    Builder() {}

    public Builder qualifier(String qualifier) {
      this.qualifier = qualifier;
      return this;
    }

    public Builder streamId(Long streamId) {
      this.streamId = streamId;
      return this;
    }

    public Builder data(Object data) {
      this.data = Objects.requireNonNull(data);
      return this;
    }

    /**
     * Finally build the {@link PubSubMessage} from current builder.
     *
     * @return {@link PubSubMessage} with parameters from current builder.
     */
    public PubSubMessage build() {
      return new PubSubMessage(qualifier, streamId, data);
    }
  }

  public static PubSubMessage error(MessageCodecException e) {
    return PubSubMessage.builder().qualifier("/io.scalecube/error").data(e).build();
  }
}
