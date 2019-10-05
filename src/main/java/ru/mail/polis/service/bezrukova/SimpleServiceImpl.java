package ru.mail.polis.service.bezrukova;

import com.google.common.base.Charsets;
import one.nio.http.*;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

public class SimpleServiceImpl extends HttpServer implements Service {

    private final DAO dao;

    public SimpleServiceImpl(final int port, @NotNull final DAO dao) throws IOException {
        super(getConfig(port));
        this.dao = dao;
    }

    private static HttpServerConfig getConfig(final int port) {
        if (port <= 1024 || port >= 66535) {
            throw new IllegalArgumentException("Invalid port");
        }

        AcceptorConfig acceptor = new AcceptorConfig();
        HttpServerConfig config = new HttpServerConfig();
        acceptor.port = port;
        config.acceptors = new AcceptorConfig[]{acceptor};
        return config;
    }

    @Path("/v0/status")
    public Response status() {
        return new Response(Response.OK, Response.EMPTY);
    }

    @Path("/v0/entity")
    public Response entity(@Param("id") final String id, @NotNull final Request request) {
        if (id == null || id.isEmpty()) {
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }

        ByteBuffer key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));

        try {
            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    return get(key);

                case Request.METHOD_PUT:
                    return put(key, request);

                case Request.METHOD_DELETE:
                    return delete(key);

                default:
                    return new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY);
            }
        } catch (IOException exception) {
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
    }

    //return 200 OK and value or 404 Not Found
    private Response get(ByteBuffer key) throws IOException {
        try {
            ByteBuffer copy = dao.get(key).duplicate();
            byte[] value = new byte[copy.remaining()];
            copy.get(value);
            return new Response(Response.OK, value);
        } catch (NoSuchElementException e) {
            return new Response(Response.NOT_FOUND, "Key was not found".getBytes(Charsets.UTF_8));
        }
    }

    //return 201  Created
    private Response put(ByteBuffer key, Request request) throws IOException {
        dao.upsert(key, ByteBuffer.wrap(request.getBody()));
        return new Response(Response.CREATED, Response.EMPTY);
    }

    //return 202  Accepted
    private Response delete(ByteBuffer key) throws IOException {
        dao.remove(key);
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
    }
}
