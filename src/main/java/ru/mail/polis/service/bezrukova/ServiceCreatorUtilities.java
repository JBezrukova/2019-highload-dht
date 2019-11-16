package ru.mail.polis.service.bezrukova;

import one.nio.http.HttpServerConfig;
import one.nio.server.AcceptorConfig;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.net.http.HttpClient;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;

public final class ServiceCreatorUtilities {

    private ServiceCreatorUtilities() {
    }

    /**
     * Creatimg a Service.
     *
     * @param port     - int
     * @param nodes    - Topology
     * @param dao      - DAO
     * @param executor - Executor
     * @return ReplicasSimpleServiceImpl
     * @throws IOException is possible
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
        httpServerConfig.queueTime = 12;
        return new ReplicasSimpleServiceImpl(httpServerConfig, dao, executor, nodes);
    }

    /**
     * Initiatializing a pool.
     *
     * @param topology - Topology
     * @return Map with keys as String and values as HttpClient
     */
    public static Map<String, HttpClient> initPool(final Topology<String> topology) {
        final Map<String, HttpClient> pool = new HashMap<>();
        for (final String node : topology.all()) {
            if (topology.isMe(node)) {
                continue;
            }
            assert !pool.containsKey(node);
            pool.put(node, HttpClient.newBuilder().build());
        }
        return pool;
    }
}
