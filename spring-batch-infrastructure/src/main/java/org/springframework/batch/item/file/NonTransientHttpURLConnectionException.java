package org.springframework.batch.item.file;

public class NonTransientHttpURLConnectionException extends NonTransientFlatFileException {
    public NonTransientHttpURLConnectionException(String message, String input) {
        super(message, input);
    }

    public NonTransientHttpURLConnectionException(String message, String input, int lineNumber) {
        super(message, input, lineNumber);
    }

    public NonTransientHttpURLConnectionException(String message, Throwable cause, String input, int lineNumber) {
        super(message, cause, input, lineNumber);
    }
}
