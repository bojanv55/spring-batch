package org.springframework.batch.item.file;

import com.fasterxml.jackson.core.JsonParseException;
import org.junit.Test;
import org.springframework.core.io.ByteArrayResource;

import java.io.BufferedReader;

import static org.junit.Assert.assertEquals;

public class JsonBufferedReaderFactoryTests {
    @Test
    public void testCreate() throws Exception {
        JsonBufferedReaderFactory factory = new JsonBufferedReaderFactory();
        BufferedReader reader = factory.create(new ByteArrayResource("[{\"age\":1,\"name\":\"name\"},{\"age\":1,\"name\":\"name\"}]".getBytes()), "UTF-8");
        assertEquals("{\"age\":1,\"name\":\"name\"}", reader.readLine());
    }

    @Test
    public void testCreateWithErroneousObject() throws Exception {
        JsonBufferedReaderFactory factory = new JsonBufferedReaderFactory();
        BufferedReader reader = factory.create(new ByteArrayResource("[{\"age\":1,\"name\":\"name\"},{\"age\":1,\"name\":".getBytes()), "UTF-8");
        assertEquals("{\"age\":1,\"name\":\"name\"}", reader.readLine());
    }

    @Test
    public void testCreateWithExceptionOnErroneousObject() throws Exception {
        JsonBufferedReaderFactory factory = new JsonBufferedReaderFactory();
        @SuppressWarnings("resource")
        BufferedReader reader = factory.create(new ByteArrayResource("[{\"age\":1,\"name\":\"name\"},{\"age\":1,\"name\":".getBytes()), "UTF-8");
        assertEquals("{\"age\":1,\"name\":\"name\"}", reader.readLine());
        try {
            assertEquals("{\"age\":1,\"name\":\"name\"}", reader.readLine());
        }
        catch (Exception e){
            assertEquals(e instanceof JsonParseException, true);
        }
    }

    @Test
    public void testCreateWithoutArray() throws Exception {
        JsonBufferedReaderFactory factory = new JsonBufferedReaderFactory();
        try {
            factory.create(new ByteArrayResource("{\"age\":1,\"name\":\"name\"}".getBytes()), "UTF-8");
        }
        catch (Exception e){
            assertEquals("Expected an array", e.getMessage());
        }
    }
}
