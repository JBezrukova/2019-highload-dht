package ru.mail.polis.service.bezrukova;

import com.google.common.base.Splitter;
import one.nio.http.HttpSession;
import one.nio.http.Response;

import java.io.IOException;
import java.util.List;

class RF {
    private final int ack;
    private final int from;

    RF(final int ack, final int from) {
        this.ack = ack;
        this.from = from;
    }

    private static RF of(final String value) {
        final List<String> values = Splitter.on('/').splitToList(value.replace("=", ""));
        if (values.size() != 2) {
            throw new IllegalArgumentException("wrong value size");
        }
        if (Integer.parseInt(values.get(0)) < 1 || Integer.parseInt(values.get(1)) < 1) {
            throw new IllegalArgumentException("wrong values");
        }
        if (Integer.parseInt(values.get(0)) > Integer.parseInt(values.get(1))) {
            throw new IllegalArgumentException("First value must be less than second");
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
