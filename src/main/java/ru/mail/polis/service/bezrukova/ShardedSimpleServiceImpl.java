package ru.mail.polis.service.bezrukova;

import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import one.nio.net.Socket;
import one.nio.pool.PoolException;
import one.nio.server.AcceptorConfig;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.SimpleDAOImpl;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ShardedSimpleServiceImpl extends HttpServer implements Service {

    private final DAO dao;
    private final Executor executor;
    private final Logger logger = Logger.getLogger(AsyncSimpleServiceImpl.class.getName());
    private final Topology<String> topology;
    private final Map<String, HttpClient> pool;

    /**
     * Creating ShardedSimpleServiceImpl.
     *
     * @param port     - final int
     * @param dao      - final DAO
     * @param executor - final Executor
     * @param topology - final Topology
     * @throws IOException throws Input/Output exception
     */
    public ShardedSimpleServiceImpl(final int port,
                                    final DAO dao,
                                    final Executor executor,
                                    final Topology<String> topology) throws IOException {
        super(from(port));
        this.dao = dao;
        this.executor = executor;
        this.topology = topology;
        pool = new HashMap<>();
        initPool(topology);
    }

    private void initPool(final Topology<String> topology) {
        for (final String node : topology.all()) {
            if (topology.isMe(node)) {
                continue;
            }
            assert !pool.containsKey(node);
            pool.put(node, new HttpClient(new ConnectionString(node + "?timeout=100")));
        }
    }

    private static HttpServerConfig from(final int port) {
        final AcceptorConfig acceptorConfig = new AcceptorConfig();
        final HttpServerConfig config = new HttpServerConfig();
        acceptorConfig.port = port;
        acceptorConfig.reusePort = true;
        acceptorConfig.deferAccept = true;
        config.acceptors = new AcceptorConfig[]{acceptorConfig};
        return config;
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

    private void entity(final Request request, final HttpSession session) throws IOException {
        final String id = request.getParameter("id=");
        if (id == null || id.isEmpty()) {
            session.sendError(Response.BAD_REQUEST, "empty id");
            return;
        }

        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(Charset.defaultCharset()));
        final String primary = topology.primaryFor(key);

        try {
            if (!topology.isMe(primary)) {
                executeAsync(session, () -> proxy(primary, request));
                return;
            }

            final int method = request.getMethod();
            switch (method) {
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
        final String key = "start=";
        final String startParameter = request.getParameter(key);

        if (startParameter == null || startParameter.isEmpty()) {
            session.sendError(Response.BAD_REQUEST, "No start");
            return;
        }

        if (request.getMethod() != Request.METHOD_GET) {
            session.sendError(Response.METHOD_NOT_ALLOWED, "No method found");
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

    private Response proxy(final String node, final Request request) throws IOException {
        assert !topology.isMe(node);
        try {
            return pool.get(node).invoke(request);
        } catch (InterruptedException | PoolException | HttpException e) {
            throw new IOException("Can't proxy", e);
        }
    }

    private Response get(final String id) throws IOException {
        try {
            final ByteBuffer wrap = ByteBuffer.wrap(id.getBytes(Charset.defaultCharset()));
            final byte[] bytes = SimpleDAOImpl.getArray(dao.get(wrap));
            return new Response(Response.OK, bytes);
        } catch (NoSuchElementException e) {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }

    private Response delete(final String id) throws IOException {
        dao.remove(ByteBuffer.wrap(id.getBytes(Charset.defaultCharset())));
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    private Response upsert(final String id, final byte[] value) throws IOException {
        dao.upsert(ByteBuffer.wrap(id.getBytes(Charset.defaultCharset())), ByteBuffer.wrap(value));
        return new Response(Response.CREATED, Response.EMPTY);
    }

    @Override
    public void handleDefault(final Request request, final HttpSession session) throws IOException {
        final String path = request.getPath();
        switch (path) {
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

    private void executeAsync(final HttpSession httpSession, final Action action) {
        executor.execute(() -> {
            execute(httpSession, action);
        });
    }

    private void execute(final HttpSession httpSession, final Action action) {
        try {
            httpSession.sendResponse(action.act());
        } catch (IOException e) {
            try {
                httpSession.sendError(Response.INTERNAL_ERROR, e.getMessage());
            } catch (IOException ex) {
                logger.log(Level.INFO, ex.getMessage());
            }
        }
    }
}
