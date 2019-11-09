package ru.mail.polis.service.bezrukova;

import java.io.IOException;
import java.nio.ByteBuffer;

public final class Value {

    private final boolean isDeleted;
    private final long timestamp;
    private final ByteBuffer data;

    private Value(final long timestamp,
                  final ByteBuffer data,
                  final boolean isDeleted) {
        this.timestamp = timestamp;
        this.data = data;
        this.isDeleted = isDeleted;
    }

    public static Value createPresent(final ByteBuffer present, final long timestamp) {
        return new Value(timestamp, present, false);
    }

    public static Value createDeleted(final long timestamp) {
        return new Value(timestamp, ByteBuffer.allocate(0), true);
    }

    static Value createAbsent() {
        return new Value(-1, null, false);
    }

    boolean isDeleted() {
        return isDeleted;
    }

    boolean isAbsent() {
        return data == null;
    }

    long getTimestamp() {
        return timestamp;
    }

    private ByteBuffer getData() throws IOException {
        if (isDeleted) {
            throw new IOException("Is not present");
        } else {
            return data;
        }
    }

    byte[] getDataBytes() throws IOException {
        final ByteBuffer byteBuffer = getData().duplicate();
        final byte[] bytes = new byte[byteBuffer.remaining()];
        byteBuffer.get(bytes);
        return bytes;
    }

    /**
     * Getting a Value with timestamp from bytes.
     *
     * @param bytes - byte array
     * @return Valye with timestamp.
     */

    public static Value fromBytes(final byte[] bytes) {
        final ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        final short isDeleted = byteBuffer.getShort();
        final boolean isDeletedB;
        isDeletedB = isDeleted == 1;
        final long timestamp = byteBuffer.getLong();
        return new Value(timestamp, byteBuffer, isDeletedB);
    }

    /**
     * Creating a byte array from Value with timestamp.
     *
     * @return byte array
     */
    public byte[] toBytes() {
        short deleted;
        if (isDeleted) {
            deleted = 1;
        } else {
            deleted = -1;
        }
        return ByteBuffer.allocate(Short.BYTES + Long.BYTES + data.remaining())
                .putShort(deleted)
                .putLong(timestamp)
                .put(data.duplicate()).array();
    }
}
