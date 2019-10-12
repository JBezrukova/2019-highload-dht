package ru.mail.polis.dao;

import java.util.NoSuchElementException;

public class SimpleNoSuchElementException extends NoSuchElementException {

    SimpleNoSuchElementException(final String message) {
        super(message);
    }

    @Override
    public Throwable fillInStackTrace() {
        synchronized (this) {
            return this;
        }
    }
}
