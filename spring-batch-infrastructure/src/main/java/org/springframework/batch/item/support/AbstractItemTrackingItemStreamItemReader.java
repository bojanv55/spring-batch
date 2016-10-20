package org.springframework.batch.item.support;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.util.Assert;

import java.io.IOException;
import java.util.Objects;

/**
 * Abstract superclass for {@link ItemReader}s that supports restart by storing
 * serialized item in the {@link ExecutionContext} (therefore requires item ordering
 * to be preserved between runs). Item must implement equals method in order to be
 * located next time.
 *
 * Subclasses are inherently <b>not</b> thread-safe.
 *
 */
public abstract class AbstractItemTrackingItemStreamItemReader<T> extends AbstractItemStreamItemReader<T> {

    private final Class<T> itemType;
    private static final String CURRENT_ITEM = "item.current";
    private T currentItem;
    private boolean saveState = true;
    private final ObjectMapper mapper = new ObjectMapper();

    public AbstractItemTrackingItemStreamItemReader(Class<T> itemType) {
        this.itemType = itemType;
    }

    /**
     * Set the flag that determines whether to save internal data for
     * {@link ExecutionContext}. Only switch this to false if you don't want to
     * save any state from this stream, and you don't need it to be restartable.
     * Always set it to false if the reader is being used in a concurrent
     * environment.
     *
     * @param saveState flag value (default true).
     */
    public void setSaveState(boolean saveState) {
        this.saveState = saveState;
    }

    /**
     * The flag that determines whether to save internal state for restarts.
     * @return true if the flag was set
     */
    public boolean isSaveState() {
        return saveState;
    }

    protected void jumpToItem(T item) throws Exception {
        T readItem;
        do {
            readItem = read();
        } while (!Objects.equals(item, readItem));
    }

    public void setCurrentItem(T item) {
        this.currentItem = item;
    }

    /**
     * Read next item from input.
     *
     * @return item
     * @throws Exception Allows subclasses to throw checked exceptions for interpretation by the framework
     */
    protected abstract T doRead() throws Exception;

    /**
     * Open resources necessary to start reading input.
     * @throws Exception Allows subclasses to throw checked exceptions for interpretation by the framework
     */
    protected abstract void doOpen() throws Exception;

    /**
     * Close the resources opened in {@link #doOpen()}.
     * @throws Exception Allows subclasses to throw checked exceptions for interpretation by the framework
     */
    protected abstract void doClose() throws Exception;

    @Override
    public void close() {
        super.close();
        currentItem = null;
        try {
            doClose();
        } catch (Exception e) {
            throw new ItemStreamException("Error while closing item reader", e);
        }
    }

    @Override
    public void open(ExecutionContext executionContext) {
        super.open(executionContext);
        try {
            doOpen();
        }
        catch (Exception e) {
            throw new ItemStreamException("Failed to initialize the reader", e);
        }
        if (!isSaveState()) {
            return;
        }

        T item = currentItem;
        if (executionContext.containsKey(getExecutionContextKey(CURRENT_ITEM))) {
            String currentItemAsString = executionContext.getString(getExecutionContextKey(CURRENT_ITEM));
            try {
                item = mapper.readValue(currentItemAsString, itemType);
            } catch (IOException e) {
                throw  new ItemStreamException(e);
            }
        }

        if(item!=null) {
            try {
                jumpToItem(item);
            } catch (Exception e) {
                throw new ItemStreamException(e);
            }
        }

        currentItem = item;
    }

    @Override
    public T read() throws Exception {
        currentItem = doRead();
        return currentItem;
    }

    @Override
    public void update(ExecutionContext executionContext) {
        super.update(executionContext);
        if (saveState) {
            Assert.notNull(executionContext, "ExecutionContext must not be null");
            String currentItemAsString;
            try {
                currentItemAsString = mapper.writeValueAsString(currentItem);
            } catch (JsonProcessingException e) {
                throw new ItemStreamException(e);
            }
            executionContext.putString(getExecutionContextKey(CURRENT_ITEM), currentItemAsString);
        }
    }
}
