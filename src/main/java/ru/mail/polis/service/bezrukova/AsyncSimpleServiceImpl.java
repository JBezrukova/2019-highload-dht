package ru.mail.polis.service.bezrukova;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.Socket;
import one.nio.server.AcceptorConfig;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.SimpleDAOImpl;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.logging.Logger;

public class AsyncSimpleServiceImpl extends HttpServer implements Service {

    private final DAO dao;

    private final Executor executor;

    private final Logger logger = Logger.getLogger(AsyncSimpleServiceImpl.class.getName());

    /**
     * Creating AsyncSimpleServiceImpl.
     *
     * @param port     - final int
     * @param dao      - final DAO
     * @param executor - final Executor
     * @throws IOException throws Input/Output exception
     */
    public AsyncSimpleServiceImpl(final int port, final DAO dao, final Executor executor) throws IOException {
        super(from(port));
        this.dao = dao;
        this.executor = executor;
    }

    @Override
    public HttpSession createSession(final Socket socket) {
        return new StorageSession(socket, this);
    }

    @FunctionalInterface
    interface Action {
        Response act() throws IOException;
    }

    @Path("/v0/status")
    public Response status() {
        return Response.ok("OK");
    }

    private static HttpServerConfig from(final int port) {
        final AcceptorConfig ac = new AcceptorConfig();
        ac.port = port;
        ac.reusePort = true;
        ac.deferAccept = true;
        final HttpServerConfig config = new HttpServerConfig();
        config.acceptors = new AcceptorConfig[]{ac};
        return config;
    }

    Response get(final String id) throws IOException {
        try {
            final ByteBuffer value = dao.get(ByteBuffer.wrap(id.getBytes(Charset.defaultCharset())));
            final byte[] bytes = SimpleDAOImpl.getArray(value);
            return new Response(Response.OK, bytes);
        } catch (NoSuchElementException e) {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }

    Response delete(final String id) throws IOException {
        dao.remove(ByteBuffer.wrap(id.getBytes(Charset.defaultCharset())));
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    Response upsert(final String id, final byte[] value) throws IOException {
        dao.upsert(ByteBuffer.wrap(id.getBytes(Charset.defaultCharset())), ByteBuffer.wrap(value));
        return new Response(Response.CREATED, Response.EMPTY);
    }

    private void entity(final Request request, final HttpSession session) throws IOException {
        final String id = request.getParameter("id=");
        if (id == null || id.isEmpty()) {
            session.sendError(Response.BAD_REQUEST, "empty id");
            return;
        }

        try {
            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    executeAsync(session, () -> get(id));
                    break;

                case Request.METHOD_PUT:
                    executeAsync(session, () -> upsert(id, request.getBody()));
                    break;

                case Request.METHOD_DELETE:
                    executeAsync(session, () -> delete(id));
                    break;

                default:
                    session.sendError(Response.METHOD_NOT_ALLOWED, "No method found");
                    break;
            }
        } catch (IOException e) {
            session.sendError(Response.INTERNAL_ERROR, e.getMessage());
        }
    }

    private void entities(final Request request, final HttpSession session) throws IOException {
        final String start = request.getParameter("start=");

        if (start == null || start.isEmpty()) {
            session.sendError(Response.BAD_REQUEST, "No start");
            return;
        }

        if (request.getMethod() != Request.METHOD_GET) {
            session.sendError(Response.METHOD_NOT_ALLOWED, "No method found");
            return;
        }

        String end = request.getParameter("end=");
        if (end != null && end.isEmpty()) {
            end = null;
        }

        try {
            final Iterator<Record> records = dao.range(ByteBuffer.wrap(start.getBytes(Charset.defaultCharset())),
                    end == null ? null : ByteBuffer.wrap(end.getBytes(Charset.defaultCharset())));
            ((StorageSession) session).stream(records);
        } catch (IOException e) {
            session.sendError(Response.INTERNAL_ERROR, e.getMessage());
        }
    }

    @Override
    public void handleDefault(final Request request, final HttpSession session) throws IOException {
        switch (request.getPath()) {
            case "/v0/entity":
                entity(request, session);
                break;
            case "/v0/entities":
                entities(request, session);
                break;
            default:
                session.sendError(Response.BAD_REQUEST, "No pattern for path");
                break;
        }
    }

    void executeAsync(final HttpSession session, final Action action) {
        executor.execute(() -> {
            try {
                session.sendResponse(action.act());
            } catch (IOException e) {
                try {
                    session.sendError(Response.INTERNAL_ERROR, e.getMessage());
                } catch (IOException ex) {
                    logger.log(Level.INFO, ex.getMessage());
                }
            }
        });
    }
}
