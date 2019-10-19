package ru.mail.polis.service.bezrukova;

import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.Response;
import one.nio.net.Socket;
import ru.mail.polis.Record;
import ru.mail.polis.dao.SimpleDAOImpl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Iterator;

public class StorageSession extends HttpSession {

    private static final byte[] EMPTY = "0\r\n\r\n".getBytes(Charset.defaultCharset());
    private static final byte[] CRLF = "\r\n".getBytes(Charset.defaultCharset());
    private static final byte[] LF = "\n".getBytes(Charset.defaultCharset());

    private Iterator<Record> iterator;

    StorageSession(final Socket socket, final HttpServer httpServer) {
        super(socket, httpServer);
    }

    @Override
    protected void processWrite() throws Exception {
        super.processWrite();
        next();
    }

    private void next() throws IOException {
        if (iterator == null) {
            throw new IllegalStateException();
        }
        while (iterator.hasNext() && queueHead == null) {
            final Record record = iterator.next();
            final byte[] value = SimpleDAOImpl.getArray(record.getValue());
            final byte[] key = SimpleDAOImpl.getArray(record.getKey());
            final int kvLength = key.length + LF.length + value.length;
            final String kvHexString = Integer.toHexString(kvLength);
            final int chunkLength = Integer.toHexString(kvLength).length() + CRLF.length + kvLength + CRLF.length;
            writeChunk(key, value, kvHexString, chunkLength);
        }
        if (!iterator.hasNext()) {
            handleEnd();
        }
    }

    private void handleEnd() throws IOException {
        write(EMPTY, 0, EMPTY.length);
        server.incRequestsProcessed();
        if ((handling = pipeline.pollFirst()) != null) {
            if (handling == FIN) {
                scheduleClose();
            } else {
                server.handleRequest(handling, this);
            }
        }
    }

    private void writeChunk(final byte[] key,
                            final byte[] value,
                            final String keyValueString,
                            final int chunkLength) throws IOException {
        final byte[] chunkArray = new byte[chunkLength];
        final ByteBuffer byteBuffer = ByteBuffer.wrap(chunkArray);
        byteBuffer.put(keyValueString.getBytes(Charset.defaultCharset()));
        byteBuffer.put(CRLF);
        byteBuffer.put(key);
        byteBuffer.put(LF);
        byteBuffer.put(value);
        byteBuffer.put(CRLF);
        write(chunkArray, 0, chunkArray.length);
    }

    void stream(final Iterator<Record> iterator) throws IOException {
        this.iterator = iterator;
        final Response response = new Response(Response.OK);
        response.addHeader("Transfer-Encoding: chunked");
        writeResponse(response, false);
        next();
    }
}
