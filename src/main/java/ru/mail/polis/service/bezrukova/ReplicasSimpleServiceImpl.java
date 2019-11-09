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
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.SimpleDAOImpl;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ReplicasSimpleServiceImpl extends HttpServer implements Service {

    private final DAO dao;
    private final Executor executor;
    private final Logger logger = Logger.getLogger(AsyncSimpleServiceImpl.class.getName());
    private final Topology<String> topology;
    private final Map<String, HttpClient> pool;
    private final RF rf;

    private static final String PROXY_HEADER = "X-OK-Proxy: True";
    private static final String ENTITY_HEADER = "/v0/entity?id=";

    /**
     * Creating ReplicasSimpleServiceImpl.
     *
     * @param config   - final HttpServerConfig
     * @param dao      - final DAO
     * @param executor - final Executor
     * @param topology - final Topology
     * @throws IOException throws Input/Output exception
     */
    private ReplicasSimpleServiceImpl(final HttpServerConfig config,
                                     final DAO dao,
                                     final Executor executor,
                                     final Topology<String> topology) throws IOException {
        super(config);
        this.dao = dao;
        this.executor = executor;
        this.topology = topology;
        pool = new HashMap<>();
        initPool(topology);
        this.rf = new RF(topology.all().size() / 2 + 1, topology.all().size());
    }

    public static Service create(final int port,
                                 final Topology<String> nodes,
                                 final DAO dao,
                                 final Executor executor) throws IOException {
        final AcceptorConfig acceptorConfig = new AcceptorConfig();
        acceptorConfig.port = port;
        final HttpServerConfig httpServerConfig = new HttpServerConfig();
        httpServerConfig.acceptors = new AcceptorConfig[]{acceptorConfig};
        httpServerConfig.maxWorkers = Runtime.getRuntime().availableProcessors();
        httpServerConfig.queueTime = 12;
        return new ReplicasSimpleServiceImpl(httpServerConfig, dao, executor, nodes);
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
        boolean isProxied = false;
        if (request.getHeader(PROXY_HEADER) != null) {
            isProxied = true;
        }
        final RF rf = RF.getRf(request.getParameter("replicas"), session, this.rf, topology.all().size());
        if (topology.all().size() > 1) {
            try {
                switch (request.getMethod()) {
                    case Request.METHOD_GET:
                        session.sendResponse(get(request, id, rf, isProxied));
                        break;
                    case Request.METHOD_PUT:
                        session.sendResponse(upsert(request.getBody(), id, rf.getAck(), isProxied));
                        break;
                    case Request.METHOD_DELETE:
                        session.sendResponse(delete(id, rf.getAck(), isProxied));
                        break;
                    default:
                        session.sendError(Response.METHOD_NOT_ALLOWED, "No method found");
                        break;
                }
            } catch (IOException e) {
                session.sendError(Response.GATEWAY_TIMEOUT, e.getMessage());
            }
        } else {
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

    private Response get(final Request request, final String id,
                         final RF rf,
                         final boolean isProxied) throws IOException {
        int num = 0;
        String[] nodes = getReplica(ByteBuffer.wrap(id.getBytes(Charset.defaultCharset())), rf, isProxied);
        List<Value> responses = new ArrayList<>();
        for (String node : nodes) {
            try {
                Response response;
                if (topology.isMe(node)) {
                    response = getResponseIfMe(ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8)));
                } else {
                    request.addHeader(PROXY_HEADER);
                    response = pool.get(node).get(ENTITY_HEADER + id, PROXY_HEADER);
                }
                if (response.getStatus() == 404 && response.getBody().length == 0) {
                    responses.add(Value.createAbsent());
                } else if (response.getStatus() == 500) {
                    continue;
                } else {
                    responses.add(Value.fromBytes(response.getBody()));
                }
                num++;
            } catch (HttpException | PoolException | InterruptedException e) {
                logger.log(Level.INFO, e.getMessage());
            }
        }
        if (isProxied || num >= rf.getAck()) {
            return response(responses, nodes, isProxied);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    private Response response(final List<Value> responses,
                              final String[] nodes,
                              final boolean isProxied) throws IOException {
        final Value value = merge(responses);
        if (!value.isDeleted()) {
            if (isProxied) {
                if (nodes.length == 1) {
                    return new Response(Response.OK, value.toBytes());
                } else {
                    return new Response(Response.OK, value.getDataBytes());
                }
            } else {
                if (nodes.length == 1) {
                    return new Response(Response.OK, value.getDataBytes());
                } else {
                    return new Response(Response.OK, value.getDataBytes());
                }
            }
        } else if (value.isDeleted()) {
            return new Response(Response.NOT_FOUND, value.toBytes());
        } else {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }

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

    @NotNull
    private Response getResponseIfMe(ByteBuffer key) throws IOException {
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

    private Response upsert(final byte[] value, final String id, final int ack, final boolean isProxied) {
        if (isProxied) {
            try {
                dao.upsertValue(ByteBuffer.wrap(id.getBytes(Charset.defaultCharset())), ByteBuffer.wrap(value));
                return new Response(Response.CREATED, Response.EMPTY);
            } catch (IOException e) {
                return new Response(Response.INTERNAL_ERROR, e.toString().getBytes(Charset.defaultCharset()));
            }
        }

        final String[] nodes = topology.replicas(rf.getFrom(), ByteBuffer.wrap(id.getBytes(Charset.defaultCharset())));

        int acks = 0;
        for (final String node : nodes) {
            try {
                if (topology.isMe(node)) {
                    dao.upsertValue(ByteBuffer.wrap(id.getBytes(Charset.defaultCharset())), ByteBuffer.wrap(value));
                    acks++;
                } else {
                    final Response response = pool.get(node).put("/v0/entity?id=" + id, value, PROXY_HEADER);
                    if (response.getStatus() == 201) {
                        acks++;
                    }
                }
            } catch (Exception e) {
                logger.log(Level.INFO, "upsert method", e);
            }
        }
        if (acks >= ack) {
            return new Response(Response.CREATED, Response.EMPTY);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    private Response delete(final String id,
                            final int ack,
                            final boolean isProxied) {
        if (isProxied) {
            try {
                dao.removeValue(ByteBuffer.wrap(id.getBytes(Charset.defaultCharset())));
                return new Response(Response.ACCEPTED, Response.EMPTY);
            } catch (IOException e) {
                return new Response(Response.INTERNAL_ERROR, e.toString().getBytes(Charset.defaultCharset()));
            }
        }

        final String[] nodes = topology.replicas(rf.getFrom(), ByteBuffer.wrap(id.getBytes(Charset.defaultCharset())));

        int acks = 0;
        for (final String node : nodes) {
            try {
                if (topology.isMe(node)) {
                    dao.removeValue(ByteBuffer.wrap(id.getBytes(Charset.defaultCharset())));
                    acks++;
                } else {
                    final Response response = pool.get(node).delete("/v0/entity?id=" + id, PROXY_HEADER);
                    if (response.getStatus() == 202) {
                        acks++;
                    }
                }
            } catch (Exception e) {
                logger.log(Level.INFO, "upsert method", e);
            }
        }
        if (acks >= ack) {
            return new Response(Response.ACCEPTED, Response.EMPTY);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    @NotNull
    private String[] getReplica(ByteBuffer key, RF rf, boolean isProxied) {
        String[] replica;
        if (isProxied) {
            replica = new String[]{topology.getMe()};
        } else {
            replica = topology.replicas(rf.getFrom(), key);
        }
        return replica;
    }
}
