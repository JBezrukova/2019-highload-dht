package ru.mail.polis.service.bezrukova;

import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.pool.PoolException;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.SimpleDAOImpl;
import ru.mail.polis.dao.Timestamp;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;

class Coordinator {

    private static final String PROXY_HEADER = "X-OK-Proxy: True";
    private static final String ENTITY_HEADER = "/v0/entity?id=";

    private final SimpleDAOImpl dao;
    private final Map<String, HttpClient> pool;
    private final Topology<String> topology;
    private final boolean isProxied;

    private static final Logger logger = Logger.getLogger(Coordinator.class.getName());

    Coordinator(final SimpleDAOImpl dao,
                final Map<String, HttpClient> pool,
                final Topology<String> topology,
                final boolean isProxied) {
        this.dao = dao;
        this.pool = pool;
        this.topology = topology;
        this.isProxied = isProxied;
    }

    private Response get(final String[] nodes,
                         final Request request,
                         final int ack,
                         final boolean isProxied) throws IOException {
        int num = 0;
        List<Timestamp> responses = new ArrayList<>();
        String id = getParameter(request);
        final ByteBuffer key = getWrappedKey(id.getBytes(StandardCharsets.UTF_8));
        for (String node : nodes) {
            try {
                Response response;
                if (topology.isMe(node)) {
                    response = getResponseIfMe(key);
                } else {
                    request.addHeader(PROXY_HEADER);
                    response = pool.get(node).get(ENTITY_HEADER + id, PROXY_HEADER);
                }
                if (response.getStatus() == 404 && response.getBody().length == 0) {
                    responses.add(Timestamp.getAbsent());
                } else if (response.getStatus() == 500) {
                    continue;
                } else {
                    responses.add(Timestamp.fromBytes(response.getBody()));
                }
                num++;
            } catch (HttpException | PoolException | InterruptedException e) {
                logger.log(Level.INFO, e.getMessage());
            }
        }
        if (isProxied || num >= ack) {
            return response(responses, nodes);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    @NotNull
    private Response getResponseIfMe(ByteBuffer key) throws IOException {
        Response result;
        try {
            final Timestamp val = dao.getWithTimestamp(key);
            if (val.isAbsent()) {
                throw new NoSuchElementException("Is absent");
            }
            result = new Response(Response.OK, val.toBytes());
        } catch (NoSuchElementException exp) {
            result = new Response(Response.NOT_FOUND, Response.EMPTY);
        }
        return result;
    }

    @NotNull
    private ByteBuffer getWrappedKey(byte[] bytes) {
        return ByteBuffer.wrap(bytes);
    }

    private String getParameter(Request request) {
        return request.getParameter("id=");
    }

    private Response upsert(final String[] nodes,
                            final Request request,
                            final int ack,
                            final boolean isProxied) {
        int num = 0;
        final String id = getParameter(request);
        final ByteBuffer key = getWrappedKey(id.getBytes(Charset.defaultCharset()));
        for (String node : nodes) {
            try {
                if (topology.isMe(node)) {
                    dao.upsertWithTimestamp(key, getWrappedKey(request.getBody()));
                    num++;
                } else {
                    request.addHeader(PROXY_HEADER);
                    final Response response = pool.get(node).put(ENTITY_HEADER + id, request.getBody(), PROXY_HEADER);
                    if (response.getStatus() == 201) {
                        num++;
                    }
                }
            } catch (IOException | HttpException | PoolException | InterruptedException e) {
                logger.log(Level.INFO, e.getMessage());
            }
        }
        if (isProxied || num >= ack) {
            return new Response(Response.CREATED, Response.EMPTY);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    private Response delete(final String[] nodes,
                            final Request request,
                            final int ack,
                            final boolean isProxied) {
        int num = 0;
        final String id = getParameter(request);
        final ByteBuffer key = getWrappedKey(id.getBytes(StandardCharsets.UTF_8));
        for (String node : nodes) {
            try {
                if (topology.isMe(node)) {
                    dao.removeWithTimestamp(key);
                    num++;
                } else {
                    request.addHeader(PROXY_HEADER);
                    final Response response = pool.get(node).delete(ENTITY_HEADER + id, PROXY_HEADER);
                    if (response.getStatus() == 202) {
                        num++;
                    }
                }
            } catch (IOException | HttpException | InterruptedException | PoolException e) {
                logger.log(Level.INFO, e.getMessage());
            }
        }
        if (isProxied || num >= ack) {
            return new Response(Response.ACCEPTED, Response.EMPTY);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    private Response response(final List<Timestamp> responses, final String[] nodes) throws IOException {
        final Timestamp timestamp = Timestamp.merge(responses);
        if (timestamp.isPresent()) {
            if (isProxied) {
                if (nodes.length == 1) {
                    return new Response(Response.OK, timestamp.toBytes());
                } else {
                    return new Response(Response.OK, timestamp.getPresentAsBytes());
                }
            } else {
                if (nodes.length == 1) {
                    return new Response(Response.OK, timestamp.getPresentAsBytes());
                } else {
                    return new Response(Response.OK, timestamp.getPresentAsBytes());
                }
            }
        } else if (timestamp.isRemoved()) {
            return new Response(Response.NOT_FOUND, timestamp.toBytes());
        } else {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }

    void request(final String[] clusters,
                 final Request request,
                 final int ack,
                 final HttpSession session) throws IOException {
        try {
            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    session.sendResponse(get(clusters, request, ack, isProxied));
                    return;
                case Request.METHOD_PUT:
                    session.sendResponse(upsert(clusters, request, ack, isProxied));
                    return;
                case Request.METHOD_DELETE:
                    session.sendResponse(delete(clusters, request, ack, isProxied));
                    return;
                default:
                    session.sendError(Response.METHOD_NOT_ALLOWED, "No method found");
            }
        } catch (IOException e) {
            session.sendError(Response.GATEWAY_TIMEOUT, e.getMessage());
        }
    }
}
