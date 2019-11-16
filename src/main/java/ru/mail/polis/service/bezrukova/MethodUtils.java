package ru.mail.polis.service.bezrukova;

import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public final class MethodUtils {

    private static final String NO_METHOD_FOUND = "No method found";

    private MethodUtils() {
    }

    /**
     * Merging values.
     *
     * @param responses - List of responses
     * @return Value
     */
    private static Value merge(final List<Value> responses) {
        if (responses.size() == 1) {
            return responses.get(0);
        } else {
            return responses.stream()
                    .filter(value -> !value.isAbsent())
                    .max(Comparator.comparingLong(Value::getTimestamp))
                    .orElseGet(Value::createAbsent);
        }
    }

    /**
     * Getting a replica.
     *
     * @param key       - ByteBuffer
     * @param rf        - RF
     * @param isProxied - boolean
     * @param topology  - Topology
     * @return array of Strings
     */
    @NotNull
    static String[] getReplica(final ByteBuffer key,
                               final RF rf,
                               final boolean isProxied,
                               final Topology<String> topology) {
        String[] replica;
        if (isProxied) {
            replica = new String[]{topology.getMe()};
        } else {
            replica = topology.replicas(rf.getFrom(), key);
        }
        return replica;
    }

    /**
     * Getting a response.
     *
     * @param responses - list of values
     * @param nodes     - array of strings
     * @param isProxied - boolean
     * @return a Response
     * @throws IOException is possible
     */
    public static Response response(final List<Value> responses, final String[] nodes,
                                    final boolean isProxied) throws IOException {
        final Value value = MethodUtils.merge(responses);
        if (value.isDeleted()) {
            return new Response(Response.NOT_FOUND, value.toBytes());
        } else {
            if (nodes.length == 1) {
                if (isProxied) {
                    return new Response(Response.OK, value.toBytes());
                } else {
                    return new Response(Response.OK, value.getDataBytes());
                }
            } else {
                return new Response(Response.OK, value.getDataBytes());
            }
        }
    }

    /**
     * Getting a response if isMe == true.
     *
     * @param key - ByteBuffer
     * @param dao - DAO
     * @return a response
     * @throws IOException is possible
     */
    @NotNull
    static Response getResponseIfMe(final ByteBuffer key, final DAO dao) throws IOException {
        Response result;
        try {
            final Value val = dao.getValue(key);
            if (val.isAbsent()) {
                throw new NoSuchElementException("Is absent");
            }
            result = new Response(Response.OK, val.toBytes());
        } catch (NoSuchElementException exp) {
            result = new Response(Response.NOT_FOUND, Response.EMPTY);
        }
        return result;
    }

    /**
     * Method for entities.
     *
     * @param request - Request
     * @param session - session
     * @param dao - DAO
     * @throws IOException is possible
     */
    static void entities(final Request request, final HttpSession session, final DAO dao) throws IOException {
        final String key = "start=";
        final String startParameter = request.getParameter(key);
        if (startParameter == null || startParameter.isEmpty()) {
            session.sendError(Response.BAD_REQUEST, "No start");
            return;
        }
        if (request.getMethod() != Request.METHOD_GET) {
            session.sendError(Response.METHOD_NOT_ALLOWED, NO_METHOD_FOUND);
            return;
        }
        final String keyEnd = "end=";
        String end = request.getParameter(keyEnd);
        if (end != null && end.isEmpty()) {
            end = null;
        }
        try {
            final ByteBuffer wrap = ByteBuffer.wrap(startParameter.getBytes(Charset.defaultCharset()));
            final Iterator<Record> records = dao.range(wrap,
                    end == null ? null : ByteBuffer.wrap(end.getBytes(Charset.defaultCharset())));
            ((StorageSession) session).stream(records);
        } catch (IOException e) {
            session.sendError(Response.INTERNAL_ERROR, e.getMessage());
        }
    }
}
