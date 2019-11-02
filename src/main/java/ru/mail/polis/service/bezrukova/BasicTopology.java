package ru.mail.polis.service.bezrukova;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Set;

public class BasicTopology implements Topology<String> {

    @NotNull
    private final String me;
    @NotNull
    private final String[] nodes;

    /**
     * BasicTopology constructor.
     *
     * @param nodes - final Set
     * @param me    - final String
     */
    public BasicTopology(@NotNull final Set<String> nodes, @NotNull final String me) {
        assert nodes.contains(me);
        this.me = me;
        this.nodes = new String[nodes.size()];
        nodes.toArray(this.nodes);
        Arrays.sort(this.nodes);
    }

    @NotNull
    @Override
    public String primaryFor(@NotNull final ByteBuffer key) {
        final int hash = key.hashCode();
        final int node = (hash & Integer.MAX_VALUE) % nodes.length;
        return nodes[node];
    }

    @Override
    public boolean isMe(@NotNull final String node) {
        return me.equals(node);
    }

    @NotNull
    @Override
    public String getMe() {
        return me;
    }

    @NotNull
    @Override
    public Set<String> all() {
        return Set.of(nodes);
    }

    @NotNull
    @Override
    public String[] replicas(final int size, final ByteBuffer id) {
        int index = (id.hashCode() & Integer.MAX_VALUE) % nodes.length;
        final String[] result = new String[size];
        for (int j = 0; j < size; j++) {
            result[j] = nodes[index];
            index = (index + 1) % nodes.length;
        }
        return result;
    }
}
