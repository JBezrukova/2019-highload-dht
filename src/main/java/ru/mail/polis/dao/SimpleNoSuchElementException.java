package ru.mail.polis.dao;

import java.util.NoSuchElementException;

public class SimpleNoSuchElementException extends NoSuchElementException {

    private static final long serialVersionUID = 101010L;

    SimpleNoSuchElementException(final String message) {
        super(message);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
