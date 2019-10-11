package ru.mail.polis.dao;

import java.util.NoSuchElementException;

public class SimpleNoSuchElementException extends NoSuchElementException {

    SimpleNoSuchElementException(String message) {
        super(message);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
