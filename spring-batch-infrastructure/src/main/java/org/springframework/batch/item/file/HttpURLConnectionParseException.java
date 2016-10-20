package org.springframework.batch.item.file;

public class HttpURLConnectionParseException extends FlatFileParseException {
    public HttpURLConnectionParseException(String message, String input) {
        super(message, input);
    }

    public HttpURLConnectionParseException(String message, String input, int lineNumber) {
        super(message, input, lineNumber);
    }

    public HttpURLConnectionParseException(String message, Throwable cause, String input, int lineNumber) {
        super(message, cause, input, lineNumber);
    }
}
