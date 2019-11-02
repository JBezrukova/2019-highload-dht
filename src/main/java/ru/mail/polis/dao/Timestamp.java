package ru.mail.polis.dao;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;

public class Timestamp {

    private final State state;
    private final long timestamp;
    private final ByteBuffer present;

    private Timestamp(final long timestamp,
                      final ByteBuffer present,
                      final State type) {
        this.timestamp = timestamp;
        this.state = type;
        this.present = present;
    }

    static Timestamp fromPresent(final ByteBuffer present, final long timestamp) {
        return new Timestamp(timestamp, present, State.PRESENT);
    }

    static Timestamp timestamp(final long timestamp) {
        return new Timestamp(timestamp, null, State.REMOVED);
    }

    public boolean isPresent() {
        return state == State.PRESENT;
    }

    public boolean isRemoved() {
        return state == State.REMOVED;
    }

    public boolean isAbsent() {
        return state == State.ABSENT;
    }

    private long getTimestamp() {
        return timestamp;
    }

    private ByteBuffer getPresent() throws IOException {
        if (isPresent()) {
            return present;
        } else {
            throw new IOException("Is not present");
        }
    }

    public byte[] getPresentAsBytes() throws IOException {
        final ByteBuffer byteBuffer = getPresent().duplicate();
        final byte[] bytes = new byte[byteBuffer.remaining()];
        byteBuffer.get(bytes);
        return bytes;
    }

    public static Timestamp merge(final List<Timestamp> responses) {
        if (responses.size() == 1) {
            return responses.get(0);
        } else {
            return responses.stream()
                    .filter(timestamp -> !timestamp.isAbsent())
                    .max(Comparator.comparingLong(Timestamp::getTimestamp))
                    .orElseGet(Timestamp::getAbsent);
        }
    }

    public static Timestamp getAbsent() {
        return new Timestamp(-1, null, State.ABSENT);
    }

    public static Timestamp fromBytes(final byte[] bytes) {
        if (bytes == null) {
            return getAbsent();
        }
        final ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        final State state = fromValue(byteBuffer.get());
        return new Timestamp(byteBuffer.getLong(), byteBuffer, state);
    }

    public byte[] toBytes() {
        int length = 0;
        if (isPresent()) {
            length = present.remaining();
        }
        final ByteBuffer byteBuffer = ByteBuffer.allocate(1 + Long.BYTES + length);
        byteBuffer.put(state.value);
        byteBuffer.putLong(getTimestamp());
        if (isPresent()) {
            byteBuffer.put(present.duplicate());
        }
        return byteBuffer.array();
    }

    enum State {
        PRESENT((byte) 1),
        REMOVED((byte) -1),
        ABSENT((byte) 0);

        final byte value;

        State(final byte value) {
            this.value = value;
        }
    }

    private static State fromValue(final byte value) {
        if (value == State.PRESENT.value) {
            return State.PRESENT;
        }
        if (value == State.REMOVED.value) {
            return State.REMOVED;
        }
        return State.ABSENT;
    }
}
