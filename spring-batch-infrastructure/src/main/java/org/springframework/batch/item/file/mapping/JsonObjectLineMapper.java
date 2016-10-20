package org.springframework.batch.item.file.mapping;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.batch.item.file.LineMapper;

/**
 * Interpret a line as a JSON object and parse it up to Object. The line should be a standard JSON object, starting with
 * "{" and ending with "}" and composed of <code>name:value</code> pairs separated by commas. Whitespace is ignored,
 * e.g.
 *
 * <pre>
 * { "foo" : "bar", "value" : 123 }
 * </pre>
 *
 * The values can also be JSON objects:
 *
 * <pre>
 * { "foo": "bar", "map": { "one": 1, "two": 2}}
 * </pre>
 *
 */
public class JsonObjectLineMapper<T> implements LineMapper<T> {
    private final Class<T> type;
    private final ObjectMapper om = new ObjectMapper();

    public JsonObjectLineMapper(Class<T> type) {
        this.type = type;
    }

    /**
     * Interpret the line as json object and deserialize it.
     * @param line to be mapped
     * @param lineNumber of the current line
     * @return de-serialized object
     * @throws Exception
     */
    @Override
    public T mapLine(String line, int lineNumber) throws Exception {
        return om.readValue(line, type);
    }
}
