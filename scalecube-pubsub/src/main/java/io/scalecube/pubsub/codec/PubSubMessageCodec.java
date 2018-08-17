package io.scalecube.pubsub.codec;

import static com.fasterxml.jackson.core.JsonToken.VALUE_NULL;
import static io.scalecube.pubsub.PubSubMessage.DATA_FIELD;
import static io.scalecube.pubsub.PubSubMessage.QUALIFIER_FIELD;
import static io.scalecube.pubsub.PubSubMessage.STREAM_ID_FIELD;
import io.scalecube.pubsub.PubSubMessage;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.util.ReferenceCountUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;

/**
 * Encodes/decodes {@link PubSubMessage} to/from {@link ByteBuf}.
 */
public class PubSubMessageCodec {
  private static final Logger LOGGER = LoggerFactory.getLogger(PubSubMessageCodec.class);

  private static final ObjectMapper objectMapper = objectMapper();

  private static final MappingJsonFactory jsonFactory = new MappingJsonFactory(objectMapper);

  /**
   * Encode given {@code message} to given {@code byteBuf}
   *
   * @param message - input message to be encoded.
   * @throws MessageCodecException in case of issues during encoding.
   */
  public ByteBuf encode(PubSubMessage message) throws MessageCodecException {
    ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer();
    try (
        JsonGenerator generator =
            jsonFactory.createGenerator((OutputStream) new ByteBufOutputStream(byteBuf), JsonEncoding.UTF8)) {
      generator.writeStartObject();

      if (message.qualifier() != null) {
        generator.writeStringField(QUALIFIER_FIELD, message.qualifier());
      }
      if (message.streamId() != null) {
        generator.writeNumberField(STREAM_ID_FIELD, message.streamId());
      }
      
      // data
      Object data = message.data();
      if (data != null) {
        if (data instanceof ByteBuf) {
          ByteBuf dataBin = (ByteBuf) data;
          if (dataBin.readableBytes() > 0) {
            generator.writeFieldName(DATA_FIELD);
            generator.writeRaw(":");
            generator.flush();
            byteBuf.writeBytes(dataBin);
          }
        } else {
          generator.writeObjectField(DATA_FIELD, data);
        }
      }

      generator.writeEndObject();
    } catch (Throwable ex) {
      ReferenceCountUtil.safeRelease(byteBuf);
      LOGGER.error("Failed to encode message: {}", message, ex);
      throw new MessageCodecException("Failed to encode message", ex);
    }
    return byteBuf;
  }


  /**
   * Decodes {@link PubSubMessage} from given {@code byteBuf}.
   *
   * @param byteBuf - contains raw {@link PubSubMessage} to be decoded.
   *
   * @return Decoded {@link PubSubMessage}.
   * @throws MessageCodecException - in case of issues during deserialization.
   */
  public PubSubMessage decode(ByteBuf byteBuf) throws MessageCodecException {
    try (InputStream stream = new ByteBufInputStream(byteBuf.slice())) {
      JsonParser jp = jsonFactory.createParser(stream);
      PubSubMessage.Builder result = PubSubMessage.builder();

      JsonToken current = jp.nextToken();
      if (current != JsonToken.START_OBJECT) {
        LOGGER.error("Root should be object: {}", byteBuf.toString(Charset.defaultCharset()));
        throw new MessageCodecException("Root should be object", null);
      }
      long dataStart = 0;
      long dataEnd = 0;
      while ((jp.nextToken()) != JsonToken.END_OBJECT) {
        String fieldName = jp.getCurrentName();
        current = jp.nextToken();
        if (current == VALUE_NULL) {
          continue;
        }
        switch (fieldName) {
          case QUALIFIER_FIELD:
            result.qualifier(jp.getValueAsString());
            break;
          case STREAM_ID_FIELD:
            result.streamId(jp.getValueAsLong());
            break;
          case DATA_FIELD:
            dataStart = jp.getTokenLocation().getByteOffset();
            if (current.isScalarValue()) {
              if (!current.isNumeric() && !current.isBoolean()) {
                jp.getValueAsString();
              }
            } else if (current.isStructStart()) {
              jp.skipChildren();
            }
            dataEnd = jp.getCurrentLocation().getByteOffset();
            break;
          default:
            break;
        }
      }
      if (dataEnd > dataStart) {
        result.data(byteBuf.slice((int) dataStart, (int) (dataEnd - dataStart)));
      }
      return result.build();
    } catch (Throwable ex) {
      LOGGER.error("Failed to decode message: {}", byteBuf.toString(Charset.defaultCharset()), ex);
      throw new MessageCodecException("Failed to decode message", ex);
    }
  }

  private static ObjectMapper objectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    mapper.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);
    mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    mapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY);
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    mapper.configure(SerializationFeature.WRITE_ENUMS_USING_TO_STRING, true);
    mapper.registerModule(new JavaTimeModule());
    return mapper;
  }
}
