package reactor.ipc.aeron;

import org.agrona.BitUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class Protocol {

    private static final int SIZE_OF_UUID = BitUtil.SIZE_OF_LONG * 2;

    public static final int HEADER_SIZE = BitUtil.SIZE_OF_BYTE + BitUtil.SIZE_OF_LONG;

    static int putHeader(MutableDirectBuffer buffer, int index, MessageType requestType, long sessionId) {
        buffer.putByte(index, (byte) requestType.ordinal());
        index += BitUtil.SIZE_OF_BYTE;

        buffer.putLong(index, sessionId);
        index += BitUtil.SIZE_OF_LONG;
        return index;
    }

    private static int putUuid(MutableDirectBuffer buffer, int index, UUID sessionId) {
        buffer.putLong(index, sessionId.getMostSignificantBits());
        index += BitUtil.SIZE_OF_LONG;
        buffer.putLong(index, sessionId.getLeastSignificantBits());
        index += BitUtil.SIZE_OF_LONG;
        return index;
    }

    public static ByteBuffer createConnectBody(UUID connectRequestId, String clientChannel, int clientControlStreamId,
                                               int clientSessionStreamId) {
        byte[] clientChannelBytes = clientChannel.getBytes(StandardCharsets.UTF_8);
        byte[] bytes = new byte[clientChannelBytes.length + BitUtil.SIZE_OF_INT * 3 + BitUtil.SIZE_OF_LONG * 2];
        UnsafeBuffer buffer = new UnsafeBuffer(bytes);
        int index = 0;
        index += putUuid(buffer, index, connectRequestId);

        buffer.putInt(index, clientChannelBytes.length);
        index += BitUtil.SIZE_OF_INT;
        buffer.putBytes(index, clientChannelBytes);
        index += clientChannelBytes.length;

        buffer.putInt(index, clientControlStreamId);
        index += BitUtil.SIZE_OF_INT;

        buffer.putInt(index, clientSessionStreamId);
        index += BitUtil.SIZE_OF_INT;

        return ByteBuffer.wrap(bytes, 0, index);
    }

    public static ByteBuffer createConnectAckBody(UUID connectRequestId, int serverSessionStreamId) {
        byte array[] = new byte[BitUtil.SIZE_OF_INT + SIZE_OF_UUID];
        UnsafeBuffer buffer = new UnsafeBuffer(array);
        int index = 0;
        buffer.putInt(index, serverSessionStreamId);
        index += BitUtil.SIZE_OF_INT;
        putUuid(buffer, index, connectRequestId);
        return ByteBuffer.wrap(array);
    }

    public static ByteBuffer createHeartbeatBody() {
        return ByteBuffer.wrap(new byte[0]);
    }

    public static ByteBuffer createDisconnectBody(long sessionId) {
        byte[] bytes = new byte[BitUtil.SIZE_OF_LONG];
        UnsafeBuffer buffer = new UnsafeBuffer(bytes);
        buffer.putLong(0, sessionId);
        return ByteBuffer.wrap(bytes);
    }
}
