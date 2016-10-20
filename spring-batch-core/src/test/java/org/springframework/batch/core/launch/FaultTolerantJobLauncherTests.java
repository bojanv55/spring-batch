/*
 * Copyright 2006-2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.batch.core.launch;

import org.junit.Before;
import org.junit.Test;
import org.springframework.batch.core.*;
import org.springframework.batch.core.job.DefaultJobParametersValidator;
import org.springframework.batch.core.job.JobSupport;
import org.springframework.batch.core.launch.support.FaultTolerantJobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.core.task.TaskExecutor;
import org.springframework.core.task.TaskRejectedException;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Lucas Ward
 * @author Will Schipp
 *
 */
public class FaultTolerantJobLauncherTests {

	private FaultTolerantJobLauncher jobLauncher;

	private RetryTemplate retryTemplate;

	private JobSupport job = new JobSupport("foo") {
		@Override
		public void execute(JobExecution execution) {
			execution.setExitStatus(ExitStatus.COMPLETED);
			return;
		}
	};

	private JobParameters jobParameters = new JobParameters();

	private JobRepository jobRepository;

	@Before
	public void setUp() throws Exception {

		retryTemplate = new RetryTemplate();
		FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
		backOffPolicy.setBackOffPeriod(1);
		SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
		retryPolicy.setMaxAttempts(2);
		retryTemplate.setRetryPolicy(retryPolicy);
		retryTemplate.setBackOffPolicy(backOffPolicy);

		jobLauncher = new FaultTolerantJobLauncher();
		jobLauncher.setRetryTemplate(retryTemplate);
		jobRepository = mock(JobRepository.class);
		jobLauncher.setJobRepository(jobRepository);

	}

	@Test
	public void testRun() throws Exception {
		run(ExitStatus.COMPLETED);
	}

	@Test(expected = JobParametersInvalidException.class)
	public void testRunWithValidator() throws Exception {

		job.setJobParametersValidator(new DefaultJobParametersValidator(new String[] { "missing-and-required" },
				new String[0]));

		when(jobRepository.getLastJobExecution(job.getName(), jobParameters)).thenReturn(null);

		jobLauncher.afterPropertiesSet();
		jobLauncher.run(job, jobParameters);

	}

	@Test
	public void testRunRestartableJobInstanceTwice() throws Exception {
		job = new JobSupport("foo") {
			@Override
			public boolean isRestartable() {
				return true;
			}

			@Override
			public void execute(JobExecution execution) {
				execution.setExitStatus(ExitStatus.COMPLETED);
				return;
			}
		};

		testRun();
		when(jobRepository.getLastJobExecution(job.getName(), jobParameters)).thenReturn(
				new JobExecution(new JobInstance(1L, job.getName()), jobParameters));
		when(jobRepository.createJobExecution(job.getName(), jobParameters)).thenReturn(
				new JobExecution(new JobInstance(1L, job.getName()), jobParameters));
		jobLauncher.run(job, jobParameters);
	}

	/*
	 * Non-restartable JobInstance can be run only once - attempt to run
	 * existing non-restartable JobInstance causes error.
	 */
	@Test
	public void testRunNonRestartableJobInstanceTwice() throws Exception {
		job = new JobSupport("foo") {
			@Override
			public boolean isRestartable() {
				return false;
			}

			@Override
			public void execute(JobExecution execution) {
				execution.setExitStatus(ExitStatus.COMPLETED);
				return;
			}
		};

		testRun();
		try {
			when(jobRepository.getLastJobExecution(job.getName(), jobParameters)).thenReturn(
					new JobExecution(new JobInstance(1L, job.getName()), jobParameters));
			jobLauncher.run(job, jobParameters);
			fail("Expected JobRestartException");
		}
		catch (JobRestartException e) {
			// expected
		}
	}

	@Test
	public void testTaskExecutor() throws Exception {
		final List<String> list = new ArrayList<String>();
		jobLauncher.setTaskExecutor(new TaskExecutor() {
			@Override
			public void execute(Runnable task) {
				list.add("execute");
				task.run();
			}
		});
		testRun();
		assertEquals(1, list.size());
	}

	@Test
	public void testTaskExecutorRejects() throws Exception {

		JobExecution jobExecution = new JobExecution((JobInstance) null, (JobParameters) null);

		final List<String> list = new ArrayList<String>();
		jobLauncher.setTaskExecutor(new TaskExecutor() {
			@Override
			public void execute(Runnable task) {
				try {
					list.add("execute");
					throw new TaskRejectedException("Planned failure");
				} catch (TaskRejectedException e) {
					jobExecution.upgradeStatus(BatchStatus.FAILED);
					if (jobExecution.getExitStatus().equals(ExitStatus.UNKNOWN)) {
						jobExecution.setExitStatus(ExitStatus.FAILED.addExitDescription(e));
					}
					jobRepository.update(jobExecution);
				}
			}
		});

		when(jobRepository.getLastJobExecution(job.getName(), jobParameters)).thenReturn(null);
		when(jobRepository.createJobExecution(job.getName(), jobParameters)).thenReturn(jobExecution);
		jobRepository.update(jobExecution);

		jobLauncher.afterPropertiesSet();
		try {
			jobLauncher.run(job, jobParameters);
		}
		finally {
			assertEquals(BatchStatus.FAILED, jobExecution.getStatus());
			assertEquals(ExitStatus.FAILED.getExitCode(), jobExecution.getExitStatus().getExitCode());
		}

		assertEquals(1, list.size());

	}

	@Test
	public void testRunWithException() throws Exception {
		job = new JobSupport() {
			@Override
			public void execute(JobExecution execution) {
				execution.setExitStatus(ExitStatus.FAILED);
				throw new RuntimeException("foo");
			}
		};
		try {
			run(ExitStatus.FAILED);
			fail("Expected RuntimeException");
		}
		catch (RuntimeException e) {
			assertEquals("Retry exhausted after last attempt with no recovery path.; nested exception is java.lang.RuntimeException: foo", e.getMessage());
		}
	}

	@Test
	public void testRunWithError() throws Exception {
		job = new JobSupport() {
			@Override
			public void execute(JobExecution execution) {
				execution.setExitStatus(ExitStatus.FAILED);
				throw new Error("foo");
			}
		};
		try {
			run(ExitStatus.FAILED);
			fail("Expected Error");
		}
		catch (RuntimeException e) {
			assertEquals("Retry exhausted after last attempt with no recovery path.; nested exception is java.lang.Error: foo", e.getMessage());
		}
	}

	@Test
	public void testInitialiseWithoutRepository() throws Exception {
		try {
			new SimpleJobLauncher().afterPropertiesSet();
			fail("Expected IllegalArgumentException");
		}
		catch (IllegalStateException e) {
			// expected
			assertTrue("Message did not contain repository: " + e.getMessage(), contains(e.getMessage().toLowerCase(),
					"repository"));
		}
	}

	@Test
	public void testInitialiseWithRepository() throws Exception {
		jobLauncher = new FaultTolerantJobLauncher();
		jobLauncher.setRetryTemplate(retryTemplate);
		jobLauncher.setJobRepository(jobRepository);
		jobLauncher.afterPropertiesSet(); // no error
	}

	private void run(ExitStatus exitStatus) throws Exception {
		JobExecution jobExecution = new JobExecution((JobInstance) null, (JobParameters) null);

		when(jobRepository.getLastJobExecution(job.getName(), jobParameters)).thenReturn(null);
		when(jobRepository.createJobExecution(job.getName(), jobParameters)).thenReturn(jobExecution);

		jobLauncher.afterPropertiesSet();
		try {
			jobLauncher.run(job, jobParameters);
		}
		finally {
			assertEquals(exitStatus, jobExecution.getExitStatus());
		}
	}

	private boolean contains(String str, String searchStr) {
		return str.indexOf(searchStr) != -1;
	}

	/**
	 * Test to support BATCH-1770 -> throw in parent thread JobRestartException when
	 * a stepExecution is UNKNOWN
	 */
	@Test(expected=JobRestartException.class)
	public void testRunStepStatusUnknown() throws Exception {
		testRestartStepExecutionInvalidStatus(BatchStatus.UNKNOWN);
	}

	@Test(expected = JobExecutionAlreadyRunningException.class)
	public void testRunStepStatusStarting() throws Exception {
		testRestartStepExecutionInvalidStatus(BatchStatus.STARTING);
	}

	@Test(expected = JobExecutionAlreadyRunningException.class)
	public void testRunStepStatusStarted() throws Exception {
		testRestartStepExecutionInvalidStatus(BatchStatus.STARTED);
	}

	@Test(expected = JobExecutionAlreadyRunningException.class)
	public void testRunStepStatusStopping() throws Exception {
		testRestartStepExecutionInvalidStatus(BatchStatus.STOPPING);
	}

	private void testRestartStepExecutionInvalidStatus(BatchStatus status) throws Exception {
		String jobName = "test_job";
		JobRepository jobRepository = mock(JobRepository.class);
		JobParameters parameters = new JobParametersBuilder().addLong("runtime", System.currentTimeMillis()).toJobParameters();
		JobExecution jobExecution = mock(JobExecution.class);
		Job job = mock(Job.class);
		JobParametersValidator validator = mock(JobParametersValidator.class);
		StepExecution stepExecution = mock(StepExecution.class);

		when(job.getName()).thenReturn(jobName);
		when(job.isRestartable()).thenReturn(true);
		when(job.getJobParametersValidator()).thenReturn(validator);
		when(jobRepository.getLastJobExecution(jobName, parameters)).thenReturn(jobExecution);
		when(stepExecution.getStatus()).thenReturn(status);
		when(jobExecution.getStepExecutions()).thenReturn(Arrays.asList(stepExecution));

		//setup launcher
		jobLauncher = new FaultTolerantJobLauncher();
		jobLauncher.setRetryTemplate(retryTemplate);
		jobLauncher.setJobRepository(jobRepository);

		//run
		jobLauncher.run(job, parameters);
	}
}
