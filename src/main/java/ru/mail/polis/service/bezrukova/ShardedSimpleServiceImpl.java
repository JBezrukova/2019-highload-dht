package ru.mail.polis.service.bezrukova;

import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import one.nio.pool.PoolException;
import ru.mail.polis.dao.DAO;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;

public class ShardedSimpleServiceImpl extends AsyncSimpleServiceImpl {

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
        super(port, dao, executor);
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

    private void entity(final Request request, final HttpSession session) throws IOException {
        final String id = request.getParameter("id=");
        if (id == null || id.isEmpty()) {
            session.sendError(Response.BAD_REQUEST, "No id");
            return;
        }

        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(Charset.defaultCharset()));
        final String primary = topology.primaryFor(key);

        if (!topology.isMe(primary)) {
            executeAsync(session, () -> proxy(primary, request));
            return;
        }

        executeRequest(request, session, id);
    }

    private Response proxy(final String node, final Request request) throws IOException {
        assert !topology.isMe(node);
        try {
            return pool.get(node).invoke(request);
        } catch (InterruptedException | PoolException | HttpException e) {
            throw new IOException("Can't proxy", e);
        }
    }

    @Override
    public void handleDefault(final Request request, final HttpSession session) throws IOException {
        final String path = request.getPath();
        final String entity = "/v0/entity";
        if (path.equals(entity)) {
            entity(request, session);
        } else {
            super.handleDefault(request, session);
        }
    }
}
