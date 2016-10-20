package org.springframework.batch.core.repository.dao;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.support.transaction.TransactionAwareProxyFactory;
import org.springframework.util.Assert;
import org.springframework.util.SerializationUtils;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

/**
 * In-memory implementation of {@link ExecutionContextDao} backed by maps. Doesn't preserve failed instances.
 *
 */
@SuppressWarnings("serial")
public class AutoCleanableMapExecutionContextDao implements ExecutionContextDao {

    private final Map<ContextKey, ExecutionContext> contexts = TransactionAwareProxyFactory
            .createTransactionalMap();

    private static final class ContextKey implements Comparable<ContextKey>, Serializable {

        private static enum Type { STEP, JOB; }

        private final AutoCleanableMapExecutionContextDao.ContextKey.Type type;
        private final long id;
        private final long jobInstance;

        private ContextKey(AutoCleanableMapExecutionContextDao.ContextKey.Type type, long id, long jobInstance) {
            if(type == null) {
                throw new IllegalStateException("Need a non-null type for a context");
            }
            this.type = type;
            this.id = id;
            this.jobInstance = jobInstance;
        }

        @Override
        public int compareTo(AutoCleanableMapExecutionContextDao.ContextKey them) {
            if(them == null) {
                return 1;
            }
            final int idCompare = new Long(this.id).compareTo(new Long(them.id)); // JDK6 Make this Long.compare(x,y)
            if(idCompare != 0) {
                return idCompare;
            }
            final int typeCompare = this.type.compareTo(them.type);
            if(typeCompare != 0) {
                return typeCompare;
            }
            return 0;
        }

        @Override
        public boolean equals(Object them) {
            if(them == null) {
                return false;
            }
            if(them instanceof AutoCleanableMapExecutionContextDao.ContextKey) {
                return this.equals((AutoCleanableMapExecutionContextDao.ContextKey)them);
            }
            return false;
        }

        public boolean equals(AutoCleanableMapExecutionContextDao.ContextKey them) {
            if(them == null) {
                return false;
            }
            return this.id == them.id && this.type.equals(them.type);
        }

        @Override
        public int hashCode() {
            int value = (int)(id^(id>>>32));
            switch(type) {
                case STEP: return value;
                case JOB: return ~value;
                default: throw new IllegalStateException("Unknown type encountered in switch: " + type);
            }
        }

        public static AutoCleanableMapExecutionContextDao.ContextKey step(long id, long jobInstance) { return new AutoCleanableMapExecutionContextDao.ContextKey(AutoCleanableMapExecutionContextDao.ContextKey.Type.STEP, id, jobInstance); }

        public static AutoCleanableMapExecutionContextDao.ContextKey job(long id, long jobInstance) { return new AutoCleanableMapExecutionContextDao.ContextKey(AutoCleanableMapExecutionContextDao.ContextKey.Type.JOB, id, jobInstance); }
    }

    public void clear() {
        contexts.clear();
    }

    private static ExecutionContext copy(ExecutionContext original) {
        return (ExecutionContext) SerializationUtils.deserialize(SerializationUtils.serialize(original));
    }

    @Override
    public ExecutionContext getExecutionContext(StepExecution stepExecution) {
        return copy(contexts.get(AutoCleanableMapExecutionContextDao.ContextKey.step(stepExecution.getId(), stepExecution.getJobExecution().getJobInstance().getInstanceId())));
    }

    @Override
    public void updateExecutionContext(StepExecution stepExecution) {
        ExecutionContext executionContext = stepExecution.getExecutionContext();
        if (executionContext != null) {
            contexts.put(AutoCleanableMapExecutionContextDao.ContextKey.step(stepExecution.getId(), stepExecution.getJobExecution().getJobInstance().getInstanceId()), copy(executionContext));
        }
    }

    @Override
    public ExecutionContext getExecutionContext(JobExecution jobExecution) {
        return copy(contexts.get(AutoCleanableMapExecutionContextDao.ContextKey.job(jobExecution.getId(), jobExecution.getJobInstance().getInstanceId())));
    }

    @Override
    public void updateExecutionContext(JobExecution jobExecution) {
        ExecutionContext executionContext = jobExecution.getExecutionContext();
        if (executionContext != null) {
            contexts.put(AutoCleanableMapExecutionContextDao.ContextKey.job(jobExecution.getId(), jobExecution.getJobInstance().getInstanceId()), copy(executionContext));
        }
    }

    @Override
    public void saveExecutionContext(JobExecution jobExecution) {

        Long jobInstanceId = jobExecution.getJobInstance().getInstanceId();
        Iterator<Map.Entry<ContextKey, ExecutionContext>> iterator = contexts.entrySet().iterator();
        while(iterator.hasNext()){
            Map.Entry<ContextKey, ExecutionContext> element = iterator.next();
            if(Objects.equals(element.getKey().type, ContextKey.Type.JOB) && Objects.equals(element.getKey().jobInstance, jobInstanceId)){
                iterator.remove();
            }
        }

        updateExecutionContext(jobExecution);
    }

    @Override
    public void saveExecutionContext(StepExecution stepExecution) {

        Long jobInstanceId = stepExecution.getJobExecution().getJobInstance().getInstanceId();
        Iterator<Map.Entry<ContextKey, ExecutionContext>> iterator = contexts.entrySet().iterator();
        while(iterator.hasNext()){
            Map.Entry<ContextKey, ExecutionContext> element = iterator.next();
            if(Objects.equals(element.getKey().type, ContextKey.Type.STEP) && Objects.equals(element.getKey().jobInstance, jobInstanceId)){
                iterator.remove();
            }
        }

        updateExecutionContext(stepExecution);
    }


    @Override
    public void saveExecutionContexts(Collection<StepExecution> stepExecutions) {
        Assert.notNull(stepExecutions,"Attempt to save a null collection of step executions");
        for (StepExecution stepExecution: stepExecutions) {
            saveExecutionContext(stepExecution);
            saveExecutionContext(stepExecution.getJobExecution());
        }
    }

}