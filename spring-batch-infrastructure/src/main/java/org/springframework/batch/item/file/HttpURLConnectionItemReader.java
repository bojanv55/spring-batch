package org.springframework.batch.item.file;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ReaderNotOpenException;
import org.springframework.batch.item.file.separator.RecordSeparatorPolicy;
import org.springframework.batch.item.file.separator.SimpleRecordSeparatorPolicy;
import org.springframework.batch.item.support.AbstractItemTrackingItemStreamItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.InputStreamResource;
import org.springframework.util.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * Restartable {@link ItemReader} that reads lines from url input {@link #setUrl(URL)}. Line is defined by the
 * {@link #setRecordSeparatorPolicy(RecordSeparatorPolicy)} and mapped to item using {@link #setLineMapper(LineMapper)}.
 * If an exception is thrown during line mapping it is rethrown as {@link HttpURLConnectionParseException} adding information
 * about the problematic line and its line number.
 *
 */
public class HttpURLConnectionItemReader<T> extends AbstractItemTrackingItemStreamItemReader<T> implements InitializingBean {

    private RecordSeparatorPolicy recordSeparatorPolicy = new SimpleRecordSeparatorPolicy();

    private BufferedReader reader;

    private int lineCount = 0;

    private String[] comments = new String[] { "#" };

    private boolean noInput = false;

    private LineMapper<T> lineMapper;

    private int linesToSkip = 0;

    private LineCallbackHandler skippedLinesCallback;

    private BufferedReaderFactory bufferedReaderFactory = new DefaultBufferedReaderFactory();

    private URL url;

    private HttpURLConnection urlConnection;

    private Map<String, String> requestProperties = new HashMap<>();

    public HttpURLConnectionItemReader(Class<T> itemType){
        super(itemType);
        setName(ClassUtils.getShortName(HttpURLConnectionItemReader.class));
    }

    /**
     * @param skippedLinesCallback will be called for each one of the initial skipped lines before any items are read.
     */
    public void setSkippedLinesCallback(LineCallbackHandler skippedLinesCallback) {
        this.skippedLinesCallback = skippedLinesCallback;
    }

    /**
     * Public setter for the number of lines to skip at the start of a file. Can be used if the file contains a header
     * without useful (column name) information, and without a comment delimiter at the beginning of the lines.
     *
     * @param linesToSkip the number of lines to skip
     */
    public void setLinesToSkip(int linesToSkip) {
        this.linesToSkip = linesToSkip;
    }

    /**
     * Setter for line mapper. This property is required to be set.
     * @param lineMapper maps line to item
     */
    public void setLineMapper(LineMapper<T> lineMapper) {
        this.lineMapper = lineMapper;
    }

    /**
     * Factory for the {@link BufferedReader} that will be used to extract lines from the file. The default is fine for
     * plain text files, but this is a useful strategy for binary files where the standard BufferedReaader from java.io
     * is limiting.
     *
     * @param bufferedReaderFactory the bufferedReaderFactory to set
     */
    public void setBufferedReaderFactory(BufferedReaderFactory bufferedReaderFactory) {
        this.bufferedReaderFactory = bufferedReaderFactory;
    }

    /**
     * Setter for comment prefixes. Can be used to ignore header lines as well by using e.g. the first couple of column
     * names as a prefix.
     *
     * @param comments an array of comment line prefixes.
     */
    public void setComments(String[] comments) {
        this.comments = new String[comments.length];
        System.arraycopy(comments, 0, this.comments, 0, comments.length);
    }

    /**
     * Public setter for the recordSeparatorPolicy. Used to determine where the line endings are and do things like
     * continue over a line ending if inside a quoted string.
     *
     * @param recordSeparatorPolicy the recordSeparatorPolicy to set
     */
    public void setRecordSeparatorPolicy(RecordSeparatorPolicy recordSeparatorPolicy) {
        this.recordSeparatorPolicy = recordSeparatorPolicy;
    }

    public void setUrl(URL url) {
        this.url = url;
    }

    private void setRequestProperties(){
        for(Map.Entry<String, String> requestProperty : requestProperties.entrySet()){
            urlConnection.setRequestProperty(requestProperty.getKey(), requestProperty.getValue());
        }
    }

    /**
     * @return string corresponding to logical record according to
     * {@link #setRecordSeparatorPolicy(RecordSeparatorPolicy)} (might span multiple lines in file).
     */
    @Override
    protected T doRead() throws Exception {
        if (noInput) {
            return null;
        }

        String line = readLine();

        if (line == null) {
            return null;
        }
        else {
            try {
                return lineMapper.mapLine(line, lineCount);
            }
            catch (Exception ex) {
                throw new HttpURLConnectionParseException("Parsing error at line: " + lineCount + " in url=["
                        + url + "], input=[" + line + "]", ex, line, lineCount);
            }
        }
    }

    /**
     * @return next line (skip comments).getCurrentResource
     */
    private String readLine() {

        if (reader == null) {
            throw new ReaderNotOpenException("Reader must be open before it can be read.");
        }

        String line = null;

        try {
            line = this.reader.readLine();
            if (line == null) {
                return null;
            }
            lineCount++;
            while (isComment(line)) {
                line = reader.readLine();
                if (line == null) {
                    return null;
                }
                lineCount++;
            }

            line = applyRecordSeparatorPolicy(line);
        }
        catch (IOException e) {
            // Prevent IOException from recurring indefinitely
            // if client keeps catching and re-calling
            noInput = true;
            throw new NonTransientHttpURLConnectionException("Unable to read from url: [" + url + "]", e, line,
                    lineCount);
        }
        return line;
    }

    private boolean isComment(String line) {
        for (String prefix : comments) {
            if (line.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected void doClose() throws Exception {
        lineCount = 0;
        if (reader != null) {
            reader.close();
        }
    }

    @Override
    protected void doOpen() throws Exception {
        Assert.notNull(url, "Url must be set");
        Assert.notNull(recordSeparatorPolicy, "RecordSeparatorPolicy must be set");

        noInput = true;

        urlConnection = (HttpURLConnection) url.openConnection();
        setRequestProperties();
        MimeType mimeType = MimeTypeUtils.parseMimeType(urlConnection.getContentType());
        Charset charset = mimeType.getCharset();
        if("gzip".equals(urlConnection.getContentEncoding())){
            reader = bufferedReaderFactory.create(new InputStreamResource(new GZIPInputStream(urlConnection.getInputStream())), charset.name());
        }
        else {
            reader = bufferedReaderFactory.create(new InputStreamResource(urlConnection.getInputStream()), charset.name());
        }

        for (int i = 0; i < linesToSkip; i++) {
            String line = readLine();
            if (skippedLinesCallback != null) {
                skippedLinesCallback.handleLine(line);
            }
        }
        noInput = false;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.notNull(lineMapper, "LineMapper is required");
    }

    private String applyRecordSeparatorPolicy(String line) throws IOException {

        String record = line;
        while (line != null && !recordSeparatorPolicy.isEndOfRecord(record)) {
            line = this.reader.readLine();
            if (line == null) {
                if (StringUtils.hasText(record)) {
                    // A record was partially complete since it hasn't ended but
                    // the line is null
                    throw new HttpURLConnectionParseException("Unexpected end of file before record complete", record, lineCount);
                }
                else {
                    // Record has no text but it might still be post processed
                    // to something (skipping preProcess since that was already
                    // done)
                    break;
                }
            }
            else {
                lineCount++;
            }
            record = recordSeparatorPolicy.preProcess(record) + line;
        }

        return recordSeparatorPolicy.postProcess(record);

    }

}
