package ru.mail.polis.service.bezrukova;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class AmmoGenerator {

    private static final int LENGTH = 256;
    private static final Logger logger = Logger.getLogger(AmmoGenerator.class.getName());
    private static final String NEW_LINE = "\r\n";

    private AmmoGenerator() {
    }

    private static byte[] randomValue() {
        final byte[] result = new byte[LENGTH];
        ThreadLocalRandom.current().nextBytes(result);
        return result;
    }

    private static void putWithUniqueKeys(final long n) throws IOException {
        for (long i = 0; i < n; i++) {
            put(Long.toString(i));
        }
    }

    private static void getWithNormalDistribution(final long n) throws IOException {
        for (long i = 0; i < n; i++) {
            get(Long.toString(ThreadLocalRandom.current().nextLong(n)));
        }
    }

    private static void putOverwrite(final long n) throws IOException {
        long key = 0;
        for (long i = 0; i < n; i++) {
            if (ThreadLocalRandom.current().nextInt(10) == 1) {
                final String keyFromList = Long.toString(ThreadLocalRandom.current().nextLong(key));
                put(keyFromList);
            } else {
                put(Long.toString(key));
                key++;
            }
        }
    }

    private static void getMostlyLast(final long n, final long records) throws IOException {
        for (long i = 0; i < n; i++) {
            final long key = lastKey(records);
            get(Long.toString(key));
        }
    }

    private static long lastKey(final long records) {
        final long half = records / 2;
        final int index = ThreadLocalRandom.current().nextInt(0, 5);
        if (index == 0) {
            return ThreadLocalRandom.current().nextLong(0, half);
        }
        if (index == 1) {
            return ThreadLocalRandom.current().nextLong(half, half + half / 2);
        } else {
            return ThreadLocalRandom.current().nextLong(half + half / 2, records);
        }
    }

    private static void putAndGet(final long n) throws IOException {
        long key = 0;
        put(Long.toString(key));
        key++;
        for (long i = 1; i < n; i++) {
            final boolean actionPut = ThreadLocalRandom.current().nextBoolean();
            if (actionPut) {
                put(Long.toString(key));
                key++;
            } else {
                get(Long.toString(ThreadLocalRandom.current().nextLong(key)));
            }
        }
    }

    private static void put(final String key) throws IOException {
        final ByteArrayOutputStream stream = new ByteArrayOutputStream();
        final byte[] val = randomValue();
        try (Writer writer = new OutputStreamWriter(stream, StandardCharsets.US_ASCII)) {
            writer.write("PUT /v0/entity?id=" + key + " HTTP/1.1" + NEW_LINE);
            writer.write("Content-Length: " + val.length + NEW_LINE);
            writer.write(NEW_LINE);
        }
        stream.write(val);
        System.out.write(Integer.toString(stream.size()).getBytes(StandardCharsets.US_ASCII));
        System.out.write(" put\n".getBytes(StandardCharsets.US_ASCII));
        stream.writeTo(System.out);
        System.out.write(NEW_LINE.getBytes(StandardCharsets.US_ASCII));
    }

    private static void get(final String key) throws IOException {
        final ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try (Writer writer = new OutputStreamWriter(stream, StandardCharsets.US_ASCII)) {
            writer.write("GET /v0/entity?id=" + key + " HTTP/1.1" + NEW_LINE);
            writer.write(NEW_LINE);
        } catch (IOException e) {
            logger.log(Level.INFO, e.getMessage());
        }
        System.out.write(Integer.toString(stream.size()).getBytes(StandardCharsets.US_ASCII));
        System.out.write(" get\n".getBytes(StandardCharsets.US_ASCII));
        stream.writeTo(System.out);
        System.out.write(NEW_LINE.getBytes(StandardCharsets.US_ASCII));
    }

    /**
     * Main method of AmmoGenerator.
     *
     * @param args - method and size of requests
     * @throws IOException can be caused by Writers in gwt and put
     */
    public static void main(final String[] args) throws IOException {
        if (args.length > 3 || args.length < 2) {
            logger.log(Level.INFO, "error in arguments");
            System.exit(-1);
        }
        final String mode = args[0];
        final int requests = Integer.parseInt(args[1]);
        switch (mode) {
            case "putWithUniqueKeys":
                putWithUniqueKeys(requests);
                break;
            case "getWithNormalDistribution":
                getWithNormalDistribution(requests);
                break;
            case "putOverwrite":
                putOverwrite(requests);
                break;
            case "getMostlyLast":
                getMostlyLast(requests, Long.parseLong(args[2]));
                break;
            case "putAndGet":
                putAndGet(requests);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported operation " + mode);
        }
    }
}
