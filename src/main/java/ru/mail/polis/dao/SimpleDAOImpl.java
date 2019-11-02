package ru.mail.polis.dao;

import static java.lang.Byte.MIN_VALUE;
import org.jetbrains.annotations.NotNull;

import static org.rocksdb.BuiltinComparator.BYTEWISE_COMPARATOR;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class SimpleDAOImpl implements DAO {

    private final RocksDB rocksDB;

    private SimpleDAOImpl(final RocksDB rocksDB) {
        this.rocksDB = rocksDB;
    }

    private byte[] convertValuesSubMinValue(final ByteBuffer byteBuffer) {
        synchronized (this) {
            final byte[] array = getArray(byteBuffer);
            for (int i = 0; i < array.length; i++) {
                array[i] -= MIN_VALUE;
            }
            return array;
        }
    }

    private static ByteBuffer convertValuesAddMinValue(final byte[] array) {
        final byte[] clone = array.clone();
        for (int i = 0; i < array.length; i++) {
            clone[i] += MIN_VALUE;
        }
        return ByteBuffer.wrap(clone);
    }

    /**
     * Getting a byte array.
     *
     * @param buffer - final ByteBuffer
     * @return byte array
     */
    public static byte[] getArray(final ByteBuffer buffer) {
        final ByteBuffer copy = buffer.duplicate();
        final byte[] value = new byte[copy.remaining()];
        copy.get(value);
        return value;
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) {
        final RocksIterator rocksIterator = rocksDB.newIterator();
        final byte[] array = convertValuesSubMinValue(from);
        rocksIterator.seek(array);

        return new IteratorImpl(rocksIterator);
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        try {
            final byte[] keyArray = convertValuesSubMinValue(key);
            final byte[] valueArray = getArray(value);
            rocksDB.put(keyArray, valueArray);
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    /**
     * Upsert with timestamps.
     *
     * @param key   - final ByteBuffer
     * @param value - final ByteBuffer
     * @throws IOException - throws an exception if RocksDBException exists.
     */
    public void upsertWithTimestamp(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        try {
            final byte[] convertedKey = convertValuesSubMinValue(key);
            final byte[] timestamp = Timestamp.fromPresent(value, System.currentTimeMillis()).toBytes();
            rocksDB.put(convertedKey, timestamp);
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        try {
            final byte[] array = convertValuesSubMinValue(key);
            rocksDB.delete(array);
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    /**
     * Remove with timestamps.
     *
     * @param key - ByteBuffer key
     * @throws IOException - throws an exception if RocksDBException exists.
     */
    public void removeWithTimestamp(@NotNull final ByteBuffer key) throws IOException {
        try {
            final byte[] convertedKey = convertValuesSubMinValue(key);
            final byte[] value = Timestamp.timestamp(System.currentTimeMillis()).toBytes();
            rocksDB.put(convertedKey, value);
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    @NotNull
    @Override
    public ByteBuffer get(@NotNull final ByteBuffer key) throws IOException, NoSuchElementException {
        try {
            final byte[] value = getValue(key);
            return ByteBuffer.wrap(value);
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    private byte[] getValue(@NotNull final ByteBuffer key) throws RocksDBException {
        final byte[] array = convertValuesSubMinValue(key);
        final byte[] value = rocksDB.get(array);
        if (value == null) {
            throw new SimpleNoSuchElementException("No element for given key " + key.toString());
        }
        return value;
    }

    /**
     * Get with timestamps.
     *
     * @param key - final ByteBuffer
     * @return Timestamp
     * @throws IOException            - throws an exception if RocksDBException exists.
     * @throws NoSuchElementException - throws an exception if RocksDBException exists
     */
    public Timestamp getWithTimestamp(@NotNull final ByteBuffer key) throws IOException, NoSuchElementException {
        try {
            final byte[] value = getValue(key);
            return Timestamp.fromBytes(value);
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() {
        rocksDB.close();
    }

    static SimpleDAOImpl init(final File data) throws IOException {
        final Options options = new Options();
        options.setComparator(BYTEWISE_COMPARATOR);
        options.setCreateIfMissing(true);
        try {
            final RocksDB rocksDB = RocksDB.open(options, data.getPath());
            return new SimpleDAOImpl(rocksDB);
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void compact() throws IOException {
        try {
            rocksDB.compactRange();
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    public static class IteratorImpl implements Iterator<Record> {

        private final RocksIterator iterator;

        IteratorImpl(final RocksIterator rocksIterator) {
            iterator = rocksIterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.isValid();
        }

        @Override
        public Record next() {
            if (hasNext()) {
                final ByteBuffer key = SimpleDAOImpl.convertValuesAddMinValue(iterator.key());
                final Record record = Record.of(key, ByteBuffer.wrap(iterator.value()));
                iterator.next();
                return record;
            } else {
                throw new IllegalStateException("End of file");
            }
        }
    }
}
