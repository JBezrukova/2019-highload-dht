package ru.mail.polis.service.bezrukova;

import com.google.common.base.Splitter;
import one.nio.http.HttpClient;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import one.nio.net.Socket;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.SimpleDAOImpl;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ReplicasSimpleServiceImpl extends HttpServer implements Service {

    private static final String PROXY_HEADER = "X-OK-Proxy: True";

    private final SimpleDAOImpl dao;
    private final Topology<String> topology;
    private final Map<String, HttpClient> pool = new HashMap<>();
    private final Executor executor;
    private final RF rf;
    private final int size;
    private final Logger logger = Logger.getLogger(AsyncSimpleServiceImpl.class.getName());

    /**
     * Creating ShardedSimpleServiceImpl.
     *
     * @param config   - final HttpServerConfig
     * @param dao      - final DAO
     * @param topology - final Topology
     * @param executor - final Executor
     * @throws IOException throws Input/Output exception
     */
    private ReplicasSimpleServiceImpl(final HttpServerConfig config,
                                      final DAO dao,
                                      final Topology<String> topology,
                                      final Executor executor) throws IOException {
        super(config);
        this.dao = (SimpleDAOImpl) dao;
        this.topology = topology;
        this.executor = executor;
        initPool(topology);
        this.size = topology.all().size();
        this.rf = new RF(topology.all().size() / 2 + 1, topology.all().size());
    }

    @FunctionalInterface
    interface Action {
        Response act() throws IOException;
    }

    /**
     * Creating a Service.
     *
     * @param port     - final int
     * @param nodes    - final Topology
     * @param dao      - final DAO
     * @param executor - final Executor
     * @return - ReplicasSimpleServiceImpl
     * @throws IOException - throws an exception from ReplicasSimpleServiceImpl
     */
    public static Service create(final int port,
                                 final Topology<String> nodes,
                                 final DAO dao,
                                 final Executor executor) throws IOException {
        final AcceptorConfig acceptorConfig = new AcceptorConfig();
        acceptorConfig.port = port;
        final HttpServerConfig httpServerConfig = new HttpServerConfig();
        httpServerConfig.acceptors = new AcceptorConfig[]{acceptorConfig};
        httpServerConfig.maxWorkers = Runtime.getRuntime().availableProcessors();
        httpServerConfig.queueTime = 10;
        return new ReplicasSimpleServiceImpl(httpServerConfig, dao, nodes, executor);
    }

    private void initPool(@NotNull final Topology<String> topology) {
        for (final String node : topology.all()) {
            if (topology.isMe(node)) {
                continue;
            }
            if (pool.containsKey(node)) throw new AssertionError();
            pool.put(node, new HttpClient(new ConnectionString(node + "?timeout=100")));
        }
    }

    @Override
    public HttpSession createSession(final Socket socket) {
        return new StorageSession(socket, this);
    }

    @Path("/v0/status")
    public Response status() {
        return Response.ok("OK");
    }

    private void entity(@NotNull final Request request, final HttpSession session) throws IOException {
        final String id = request.getParameter("id=");
        if (id == null || id.isEmpty()) {
            session.sendError(Response.BAD_REQUEST, "id is empty");
            return;
        }
        boolean isProxied = false;
        if (request.getHeader(PROXY_HEADER) != null) {
            isProxied = true;
        }

        final String replicas = request.getParameter("replicas");
        final RF newRf = RF.getRf(replicas, session, rf, size);
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(Charset.defaultCharset()));

        if (topology.all().size() > 1 || isProxied) {
            final Coordinator coordinators = new Coordinator(dao, pool, topology, isProxied);
            final String[] replica = getReplica(isProxied, newRf, key);
            assert newRf != null;
            coordinators.request(replica, request, newRf.getAck(), session);
        } else {
            executeRequest(request, session, id);
        }
    }

    private void executeRequest(@NotNull final Request request,
                                final HttpSession session,
                                final String id) throws IOException {
        final int method = request.getMethod();
        if (method == Request.METHOD_GET) {
            executeAsync(session, () -> get(id));
        } else if (method == Request.METHOD_PUT) {
            executeAsync(session, () -> upsert(id, request.getBody()));
        } else if (method == Request.METHOD_DELETE) {
            executeAsync(session, () -> delete(id));
        } else {
            session.sendError(Response.METHOD_NOT_ALLOWED, "No method found");
        }
    }

    @NotNull
    private String[] getReplica(final boolean isProxied, final RF rf, final ByteBuffer key) {
        String[] replica;
        if (isProxied) {
            replica = new String[]{topology.getMe()};
        } else {
            replica = topology.replicas(rf.getFrom(), key);
        }
        return replica;
    }

    private void entities(@NotNull final Request request, final HttpSession session) throws IOException {
        final String startParameter = request.getParameter("start=");

        if (startParameter == null || startParameter.isEmpty()) {
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
            final byte[] bytes = startParameter.getBytes(Charset.defaultCharset());
            final ByteBuffer wrap = ByteBuffer.wrap(bytes);
            final Iterator<Record> records = dao.range(wrap,
                    end == null ? null : ByteBuffer.wrap(end.getBytes(Charset.defaultCharset())));
            ((StorageSession) session).stream(records);
        } catch (IOException e) {
            session.sendError(Response.INTERNAL_ERROR, e.getMessage());
        }
    }

    private Response get(final String id) throws IOException {
        try {
            final byte[] bytesArray = id.getBytes(Charset.defaultCharset());
            final ByteBuffer wrap = ByteBuffer.wrap(bytesArray);
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
        final String entity = "/v0/entity";
        final String entities = "/v0/entities";
        switch (path) {
            case entity:
                entity(request, session);
                break;
            case entities:
                entities(request, session);
                break;
            default:
                session.sendError(Response.BAD_REQUEST, "No pattern for path");
                break;
        }
    }

    private void executeAsync(final HttpSession httpSession, final Action action) {
        executor.execute(() -> execute(httpSession, action));
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

    private static class RF {
        private final int ack;
        private final int from;

        RF(final int ack, final int from) {
            this.ack = ack;
            this.from = from;
        }

        private static RF of(final String value) {
            final List<String> values = Splitter.on('/').splitToList(value.replace("=", ""));
            if (values.size() != 2) {
                throw new IllegalArgumentException();
            }
            return new RF(Integer.parseInt(values.get(0)), Integer.parseInt(values.get(1)));
        }

        static RF getRf(final String replicas,
                        final HttpSession session,
                        final RF rf,
                        final int size) throws IOException {
            RF newRf = null;
            try {
                if (replicas == null) {
                    newRf = rf;
                } else {
                    newRf = RF.of(replicas);
                }
                if (newRf.ack < 1 || newRf.from < newRf.ack || newRf.from > size) {
                    throw new IllegalArgumentException();
                }
                return newRf;
            } catch (IllegalArgumentException e) {
                session.sendError(Response.BAD_REQUEST, "wrong RF");
            }
            return newRf;
        }

        int getFrom() {
            return from;
        }

        int getAck() {
            return ack;
        }
    }
}
