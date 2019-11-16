package ru.mail.polis.service.bezrukova;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.Socket;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.SimpleDAOImpl;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ReplicasSimpleServiceImpl extends HttpServer implements Service {

    private static final String ENTITY_HEADER = "/v0/entity?id=";
    private static final String NO_METHOD_FOUND = "No method found";
    private static final int TIMEOUT = 200;
    private final DAO dao;
    private final Executor executor;
    private final Logger logger = Logger.getLogger(AsyncSimpleServiceImpl.class.getName());
    private final Topology<String> topology;
    private final Map<String, HttpClient> pool;
    private final RF rf;
    private static final String PROXY_HEADER = "PROXY_HEADER";

    /**
     * Creating ReplicasSimpleServiceImpl.
     *
     * @param config   - final HttpServerConfig
     * @param dao      - final DAO
     * @param executor - final Executor
     * @param topology - final Topology
     * @throws IOException throws Input/Output exception
     */
    public ReplicasSimpleServiceImpl(final HttpServerConfig config, final DAO dao,
                                     final Executor executor, final Topology<String> topology) throws IOException {
        super(config);
        this.dao = dao;
        this.executor = executor;
        this.topology = topology;
        pool = ServiceCreatorUtilities.initPool(topology);
        this.rf = new RF(topology.all().size() / 2 + 1, topology.all().size());
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
        final RF rf1 = RF.getRf(request.getParameter("replicas"), session, this.rf, topology.all().size());
        if (topology.all().size() > 1) {
            try {
                switch (request.getMethod()) {
                    case Request.METHOD_GET:
                        session.sendResponse(get(id, rf1, isProxied));
                        break;
                    case Request.METHOD_PUT:
                        session.sendResponse(upsert(request.getBody(), id, rf1.getAck(), isProxied));
                        break;
                    case Request.METHOD_DELETE:
                        session.sendResponse(delete(id, rf1.getAck(), isProxied));
                        break;
                    default:
                        session.sendError(Response.METHOD_NOT_ALLOWED, NO_METHOD_FOUND);
                        break;
                }
            } catch (IOException e) {
                session.sendError(Response.GATEWAY_TIMEOUT, e.getMessage());
            }
        } else {
            switch (request.getMethod()) {
                case Request.METHOD_PUT:
                    executeAsync(session, () -> upsert(id, request.getBody()));
                    break;
                case Request.METHOD_GET:
                    executeAsync(session, () -> get(id));
                    break;
                case Request.METHOD_DELETE:
                    executeAsync(session, () -> delete(id));
                    break;
                default:
                    session.sendError(Response.METHOD_NOT_ALLOWED, NO_METHOD_FOUND);
                    break;
            }
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

    private Response get(final String id,
                         final RF rf,
                         final boolean isProxied) throws IOException {
        final String[] nodes = MethodUtils.getReplica(ByteBuffer.wrap(id.getBytes(Charset.defaultCharset())),
                rf, isProxied, topology);
        final List<Value> responses = new ArrayList<>();
        final List<CompletableFuture<Value>> futures = new ArrayList<>();
        for (final String node : nodes) {
            CompletableFuture<Value> valueCompletableFuture;
            if (topology.isMe(node)) {
                final ByteBuffer byteBuffer = ByteBuffer.wrap(id.getBytes(Charset.defaultCharset()));
                valueCompletableFuture = CompletableFuture.supplyAsync(() -> {
                    try {
                        return dao.getValue(byteBuffer);
                    } catch (IOException e) {
//                        log.error(e);
                    }
                    return null;
                });
//                valueCompletableFuture = CompletableFuture.supplyAsync(MethodUtils.getSupplier(id, dao));
            } else {
                final HttpRequest httpRequest = HttpRequest
                        .newBuilder()
                        .uri(URI.create(node + ENTITY_HEADER + id))
                        .setHeader(PROXY_HEADER, "True")
                        .timeout(Duration.ofMillis(TIMEOUT))
                        .GET()
                        .build();
                valueCompletableFuture = pool
                        .get(node)
                        .sendAsync(httpRequest, HttpResponse.BodyHandlers.ofByteArray())
                        .thenApply(response -> {
                            if (response.statusCode() == 404 && response.body().length == 0) {
                                return Value.createAbsent();
                            } else if (response.statusCode() != 500) {
                                return Value.fromBytes(response.body());
                            }
                            return Value.createAbsent();
                        });
            }
            futures.add(valueCompletableFuture);
        }
        int num = 0;
        for (CompletableFuture<Value> future : futures) {
            try {
                Value value = future.get();
                if (value != null) {
                    responses.add(value);
                    num++;
                }
            } catch (InterruptedException | ExecutionException e) {
                logger.log(Level.INFO, e.getMessage());
            }
        }
        if (isProxied || num >= rf.getAck()) {
            return MethodUtils.response(responses, nodes, isProxied);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    private Response delete(final String id) throws IOException {
        dao.remove(ByteBuffer.wrap(id.getBytes(Charset.defaultCharset())));
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    private Response delete(final String id, final int ack, final boolean isProxied) {
        if (isProxied) {
            try {
                dao.removeValue(ByteBuffer.wrap(id.getBytes(Charset.defaultCharset())));
                return new Response(Response.ACCEPTED, Response.EMPTY);
            } catch (IOException e) {
                return new Response(Response.INTERNAL_ERROR, e.toString().getBytes(Charset.defaultCharset()));
            }
        }
        final String[] nodes = topology.replicas(rf.getFrom(), ByteBuffer.wrap(id.getBytes(Charset.defaultCharset())));
        final List<CompletableFuture<String>> completableFutures = new ArrayList<>();
        for (final String node : nodes) {
            final CompletableFuture<String> completableFuture;
            if (topology.isMe(node)) {
                completableFuture = CompletableFuture.runAsync(() -> {
                    try {
                        dao.removeValue(ByteBuffer.wrap(id.getBytes(Charset.defaultCharset())));
                    } catch (IOException e) {
                        logger.log(Level.INFO, e.getMessage());
                    }
                }).handle((aVoid, throwable) -> {
                    if (throwable != null) {
                        return Response.INTERNAL_ERROR;
                    }
                    return Response.ACCEPTED;
                });
            } else {
                final HttpRequest httpRequest = HttpRequest
                        .newBuilder()
                        .uri(URI.create(node + ENTITY_HEADER + id))
                        .setHeader(PROXY_HEADER, "True")
                        .timeout(Duration.ofMillis(TIMEOUT))
                        .DELETE()
                        .build();
                completableFuture = pool
                        .get(node)
                        .sendAsync(httpRequest, HttpResponse.BodyHandlers.discarding())
                        .handle((response, throwable) -> {
                            if (response.statusCode() == 202) {
                                return Response.ACCEPTED;
                            }
                            return Response.INTERNAL_ERROR;
                        });
            }
            completableFutures.add(completableFuture);
        }
        int acks = 0;
        for (CompletableFuture<String> future : completableFutures) {
            try {
                if (future.get().equals(Response.ACCEPTED)) {
                    acks++;
                }
            } catch (InterruptedException | ExecutionException e) {
                logger.log(Level.INFO, e.getMessage());
            }
        }
        if (acks >= ack) {
            return new Response(Response.ACCEPTED, Response.EMPTY);
        }
        return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
    }

    private Response upsert(final String id, final byte[] value) throws IOException {
        dao.upsert(ByteBuffer.wrap(id.getBytes(Charset.defaultCharset())), ByteBuffer.wrap(value));
        return new Response(Response.CREATED, Response.EMPTY);
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
        final List<CompletableFuture<String>> completableFutureList = new ArrayList<>();
        for (final String node : nodes) {
            final CompletableFuture<String> completableFuture;
            if (topology.isMe(node)) {
                completableFuture = CompletableFuture.runAsync(() -> {
                    try {
                        dao.upsertValue(ByteBuffer.wrap(id.getBytes(Charset.defaultCharset())), ByteBuffer.wrap(value));
                    } catch (IOException e) {
                        logger.log(Level.INFO, e.getMessage());
                    }
                }).handle((aVoid, throwable) -> {
                    if (throwable != null) {
                        return Response.INTERNAL_ERROR;
                    }
                    return Response.CREATED;
                });
            } else {
                final HttpRequest httpRequest = HttpRequest
                        .newBuilder()
                        .uri(URI.create(node + ENTITY_HEADER + id))
                        .setHeader(PROXY_HEADER, "True")
                        .timeout(Duration.ofMillis(TIMEOUT))
                        .PUT(HttpRequest.BodyPublishers.ofByteArray(value))
                        .build();
                completableFuture = pool
                        .get(node)
                        .sendAsync(httpRequest, HttpResponse.BodyHandlers.discarding())
                        .handle((response, throwable) -> {
                            if (response.statusCode() == 201) {
                                return Response.CREATED;
                            }
                            return Response.INTERNAL_ERROR;
                        });
            }
            completableFutureList.add(completableFuture);
        }
        for (CompletableFuture<String> future : completableFutureList) {
            try {
                if (future.get().equals(Response.CREATED)) {
                    acks++;
                }
            } catch (InterruptedException | ExecutionException e) {
                logger.log(Level.INFO, e.getMessage());
            }
        }
        if (acks >= ack) {
            return new Response(Response.CREATED, Response.EMPTY);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    @Override
    public void handleDefault(final Request request, final HttpSession session) throws IOException {
        final String path = request.getPath();
        switch (path) {
            case "/v0/entity":
                entity(request, session);
                break;
            case "/v0/entities":
                MethodUtils.entities(request, session, dao);
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
