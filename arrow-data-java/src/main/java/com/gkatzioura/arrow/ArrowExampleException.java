package com.gkatzioura.arrow;

public class ArrowExampleException extends RuntimeException {

    public ArrowExampleException() {
    }

    public ArrowExampleException(String message) {
        super(message);
    }

    public ArrowExampleException(String message, Throwable cause) {
        super(message, cause);
    }

    public ArrowExampleException(Throwable cause) {
        super(cause);
    }

    public ArrowExampleException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

}
