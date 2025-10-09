package com.aok.core;

public class AmqpException extends RuntimeException {
    private final int errorCode;
    private final boolean shouldCloseConnection;

    public AmqpException(int errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
        this.shouldCloseConnection = false;
    }

    public AmqpException(int errorCode, String message, boolean shouldCloseConnection) {
        super(message);
        this.errorCode = errorCode;
        this.shouldCloseConnection = shouldCloseConnection;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public boolean shouldCloseConnection() {
        return shouldCloseConnection;
    }

    public static class Codes {
        public static final int PRECONDITION_FAILED = 406;
        public static final int NOT_FOUND = 404;
        public static final int INTERNAL_ERROR = 500;
    }
}

